'use strict'

const fs = require('fs');
const path = require('path');
const _ = require('lodash');
const validationHelper = require('./validationHelper');
const mapJsonSchema = require('../reverse_engineering/helpers/mapJsonSchema');

const ADDITIONAL_PROPS = ['doc', 'order', 'aliases', 'symbols', 'namespace', 'size', 'durationSize', 'default'];
const ADDITIONAL_CHOICE_META_PROPS = ADDITIONAL_PROPS.concat('index');
const PRIMITIVE_FIELD_ATTRIBUTES = ['order', 'logicalType', 'precision', 'scale', 'aliases'];
const DEFAULT_TYPE = 'string';
const DEFAULT_NAME = 'New_field';
const VALID_FULL_NAME_REGEX = /[^A-Za-z0-9_]/g;
const VALID_FIRST_NAME_LETTER_REGEX = /^[0-9]/;
const readConfig = (pathToConfig) => {
	return JSON.parse(fs.readFileSync(path.join(__dirname, pathToConfig)).toString().replace(/(\/\*[.\s\S]*?\*\/|\/\/.*)/ig, ""));
};
const fieldLevelConfig = readConfig('../properties_pane/field_level/fieldLevelConfig.json');
let nameIndex = 0;

const LOGICAL_TYPES_MAP = {
	bytes: ['decimal'],
	int: [
		'date',
		'time-millis'
	],
	long: [
		'time-micros',
		'timestamp-millis',
		'timestamp-micros'
	],
	fixed: ['decimal', 'duration']
};

module.exports = {
	generateScript(data, logger, cb) {
		logger.clear();
		try {
			const name = getRecordName(data);
			let avroSchema = { name };
			let jsonSchema = JSON.parse(data.jsonSchema);
			const udt = getUserDefinedTypes(data);

			handleRecursiveSchema(jsonSchema, avroSchema, {}, udt);

			if (data.containerData) {
				avroSchema.namespace = data.containerData.name;
			}
			avroSchema.type = 'record';
			avroSchema = reorderAvroSchema(avroSchema);
			avroSchema = JSON.stringify(avroSchema, null, 4);
			nameIndex = 0;
			return cb(null, avroSchema);
		} catch(err) {
			nameIndex = 0;
			logger.log('error', { message: err.message, stack: err.stack }, 'Avro Forward-Engineering Error');
			cb({ message: err.message, stack: err.stack });
		}
	},
	validate(data, logger, cb) {
		try {
			const messages = validationHelper.validate(data.script);
			cb(null, messages);
		} catch (e) {
			logger.log('error', { error: e }, 'Avro Validation Error');
			cb(null, [{
				type: 'error',
				label: e.fieldName || e.name,
				title: e.message,
				context: ''
			}]);
		}
	}
};

const resolveDefinitions = definitionsSchema => {
	const definitions = _.get(definitionsSchema, 'properties', {});

	return Object.keys(definitions).reduce(resolvedDefinitions => {
		return mapJsonSchema(resolvedDefinitions, replaceReferenceByDefinitions(resolvedDefinitions))
	}, definitionsSchema);
};

const replaceReferenceByDefinitions = definitionsSchema => field => {
	if (!field.$ref) {
		return field;
	}
	const definitionName = getTypeFromReference(field);
	const definition = _.get(definitionsSchema, ['properties', definitionName]);

	if (!definition) {
		return field;
	}

	return _.cloneDeep(definition);
};

const getUserDefinedTypes = ({ internalDefinitions, externalDefinitions, modelDefinitions }) => {
	let udt = convertSchemaToUserDefinedTypes(JSON.parse(externalDefinitions), {});
	 udt = convertSchemaToUserDefinedTypes(JSON.parse(modelDefinitions), udt);
	 udt = convertSchemaToUserDefinedTypes(JSON.parse(internalDefinitions), udt);

	return udt;
};

const convertSchemaToUserDefinedTypes = (definitionsSchema, udt) => {
	const avroSchema = {};
	const jsonSchema = resolveDefinitions(definitionsSchema);

	handleRecursiveSchema(jsonSchema, avroSchema, {}, udt);

	return (avroSchema.fields || []).reduce((result, field) => {
		if (typeof field.type !== 'object') {
			return Object.assign({}, result, {
				[field.name]: field.type
			});
		}
		if (_.isArray(field.type)) {
			return Object.assign({}, result, {
				[field.name]: Object.assign({}, filterProperties(field), {
					name: field.name,
					type: field.type,
				})
			});
		}

		return Object.assign({}, result, {
			[field.name]: Object.assign({}, filterProperties(field), field.type, {
				name: field.name
			})
		});
	}, udt);
};

const filterProperties = field => {
	const redundantFieldProperties = getRedundantProperties(field);

	return _.omit(field, redundantFieldProperties);
};

const getRedundantProperties = field => Object.keys(field).filter(key => !ADDITIONAL_PROPS.includes(key));

const getRecordName = (data) => {
	return (
		data.entityData.code
		||
		data.entityData.name
		||
		data.entityData.collectionName
	);
};

const reorderAvroSchema = (avroSchema) => {
	const schemaFields = avroSchema.fields;
	delete avroSchema.fields;
	return Object.assign({}, avroSchema, {
		fields: schemaFields
	});
};

const handleRecursiveSchema = (schema, avroSchema, parentSchema = {}, udt) => {
	if (schema.oneOf) {
		handleChoice(schema, 'oneOf', udt);
	}

	if (schema.allOf) {
		handleChoice(schema, 'allOf', udt);
	}
	schema.type = schema.type || getTypeFromReference(schema);

	for (let prop in schema) {
		switch (prop) {
			case 'type':
				handleType(schema, avroSchema, udt);
				break;
			case 'properties':
				handleFields(schema, avroSchema, udt);
				break;
			case 'items':
				handleItems(schema, avroSchema, udt);
				break;
			default:
				handleOtherProps(schema, prop, avroSchema);
		}
	}
	handleComplexTypeStructure(avroSchema, parentSchema);
	handleSchemaName(avroSchema, parentSchema);
	avroSchema = reorderName(avroSchema);
	handleEmptyNestedObjects(avroSchema);
	handleTargetProperties(schema, avroSchema, parentSchema);

	handleRequired(parentSchema, avroSchema, schema);

	return;
};

const handleMergedChoice = (schema, udt) => {
	const meta = schema.allOf_meta;
	const separateChoices = meta.reduce((choices, meta) => {
		const items = schema.allOf.filter(item => {
			const ids = _.get(meta, 'ids', []);

			return ids.includes(item.GUID);
		});
		const type = _.get(meta, 'choice');
		if (!type || type === 'allOf') {
			return choices.concat({ items, type: 'allOf', meta });
		}

		const choiceItems = _.first(items)[type];

		return choices.concat({ items: choiceItems, type, meta });
		

	}, []);
	
	const newSchema = separateChoices.reduce((updatedSchema, choiceData) => {
		const choiceType = choiceData.type;
		const schemaWithChoice = Object.assign({}, removeChoices(updatedSchema), {
			[choiceType]: choiceData.items,
			[`${choiceType}_meta`]: choiceData.meta
		});

		handleChoice(schemaWithChoice, choiceType, udt);

		return schemaWithChoice;
	}, schema);

	return Object.assign(schema, newSchema);
};

const removeChoices = schema => _.omit(schema, [
	'oneOf', 'oneOf_meta', 'allOf', 'allOf_meta', 'anyOf', 'anyOf_meta', 'not', 'not_meta'
]);

const handleChoice = (schema, choice, udt) => {
	const convertDefaultMetaFieldType = (type, value) => {
		if (type === 'null' && value === 'null') {
			return null;
		}
		if (type === 'number' && !isNaN(value)) {
			return Number(value);
		}
		
		return value;
	};
	
	const choiceRawMeta = schema[`${choice}_meta`];
	if (_.isArray(choiceRawMeta)) {
		return handleMergedChoice(schema, udt);
	}

	let choiceMeta = {};
	let allSubSchemaFields = [];
	
	if (choiceRawMeta) {
		choiceMeta = Object.keys(choiceRawMeta).reduce((choiceMeta, prop) => {
			if (ADDITIONAL_CHOICE_META_PROPS.includes(prop) && typeof choiceRawMeta[prop] !== "undefined") {
				return Object.assign({}, choiceMeta, {
					[prop]: choiceRawMeta[prop]
				});
			}
			
			return choiceMeta;
		}, {});

		const choiceMetaName = choiceRawMeta.code || choiceRawMeta.name;

		if (choiceMetaName) {
			choiceMeta.name = choiceMetaName;
		}
	}
	
	schema[choice].forEach(subSchema => {
		if (subSchema.oneOf) {
			handleChoice(subSchema, 'oneOf', udt);
		}
		allSubSchemaFields = allSubSchemaFields.concat(Object.keys(subSchema.properties).map(item => {
			return Object.assign({
				name: item
			}, subSchema.properties[item]);
		}));
	});

	let multipleFieldsHash = {};

	allSubSchemaFields.forEach(field => {
		const fieldName = choiceMeta.name || field.name;
		if (!multipleFieldsHash[fieldName]) {
			if (!_.isUndefined(choiceMeta.default)) {
				choiceMeta.default = convertDefaultMetaFieldType(field.type, choiceMeta.default);
			}
			
			if (choiceMeta.default === '') {
				delete choiceMeta.default;
			}

			multipleFieldsHash[fieldName] = Object.assign({}, choiceMeta, {
				name: fieldName,
				type: [],
				choiceMeta
			});
		}
		let multipleField = multipleFieldsHash[fieldName];
		const filedType = field.type || getTypeFromReference(field) || DEFAULT_TYPE;

		if (!_.isArray(multipleField.type)) {
			multipleField.type = [multipleField.type];
		}

		if (!_.isArray(multipleField.type)) {
			multipleField.type = [multipleField.type];
		}

		if (isComplexType(filedType)) {
			let newField = {};
			handleRecursiveSchema(field, newField, {}, udt);
			newField.name = newField.name || field.name || fieldName;
			newField.type.name = newField.type.name || field.name || fieldName;
			newField.type = reorderName(newField.type);
			multipleField.type.push(newField);
		} else if (Array.isArray(filedType)) {
			multipleField.type = multipleField.type.concat(filedType);
		} else {
			multipleField.type = multipleField.type.concat([filedType]);
		}

		multipleField.type = _.uniq(multipleField.type);
		if (multipleField.type.length === 1) {
			multipleField.type = _.first(multipleField.type);
		}
	});

	schema.properties = addPropertiesFromChoices(schema.properties, multipleFieldsHash);
};

const getChoiceIndex = choice => _.get(choice, 'choiceMeta.index');

const addPropertiesFromChoices = (properties, choiceProperties) => {
	if (_.isEmpty(choiceProperties)) {
		return properties;
	}

	const sortedKeys = Object.keys(choiceProperties).sort((a, b) => {
		return getChoiceIndex(a) - getChoiceIndex(b)
	});

	return sortedKeys.reduce((sortedProperties, choicePropertyKey) => {
		const choiceProperty = choiceProperties[choicePropertyKey];
		const choicePropertyIndex = getChoiceIndex(choiceProperty);
		if (_.isEmpty(sortedProperties)) {
			return { [choicePropertyKey]: choiceProperty };
		}

		if (
			_.isUndefined(choicePropertyIndex) ||
			Object.keys(sortedProperties).length <= choicePropertyIndex
		) {
			return Object.assign({}, sortedProperties, {
				[choicePropertyKey]: choiceProperty
			});
		}

		return Object.keys(sortedProperties).reduce((result, propertyKey, index, keys) => {
			const currentIndex = getChoiceIndex(sortedProperties[propertyKey]);
			const hasSameChoiceIndex = !_.isUndefined(currentIndex) && currentIndex <= choicePropertyIndex;
			if (index < choicePropertyIndex || result[choicePropertyKey] || hasSameChoiceIndex) {
				if (!result[choicePropertyKey] && keys.length === index + 1) {
					return Object.assign({}, result, {
						[propertyKey] : sortedProperties[propertyKey],
						[choicePropertyKey]: choiceProperty,
					});
				}
				return Object.assign({}, result, {
					[propertyKey] : sortedProperties[propertyKey]
				});
			}

			return Object.assign({}, result, {
				[choicePropertyKey]: choiceProperty,
				[propertyKey] : sortedProperties[propertyKey]
			});
		}, {});
	}, properties || {});
};

const isRequired = (parentSchema, name) => {
	if (!Array.isArray(parentSchema.required)) {
		return false;
	} else {
		return parentSchema.required.some(requiredName => prepareName(requiredName) === name);
	}
};

const handleRequired = (parentSchema, avroSchema) => {
	const isReference = _.isObject(avroSchema.type);
	if (isReference && !_.isUndefined(avroSchema.default)) {
		return;
	}

	if (isRequired(parentSchema, avroSchema.name)) {
		delete avroSchema.default;
	}
};

const handleType = (schema, avroSchema, udt) => {
	if (Array.isArray(schema.type)) {
		avroSchema = handleMultiple(avroSchema, schema, 'type', udt);
	} else {
		avroSchema = getFieldWithConvertedType(avroSchema, schema, schema.type, udt);
	}
};

const handleMultiple = (avroSchema, schema, prop, udt) => {
	avroSchema[prop] = schema[prop].map(type => {
		if (type && typeof type === 'object') {
			return type.type;
		} else {
			const field = getFieldWithConvertedType({}, schema, type, udt);
			if (isComplexType(type)) {
				const fieldName = field.typeName || schema.name;
				const fieldProperties = getMultipleComplexTypeProperties(schema, type);

				Object.keys(fieldProperties).forEach(prop => {
					delete schema[prop];
				});

				return Object.assign({
					name: fieldName,
					type
				}, fieldProperties)
			}

			const fieldAttributesKeys = PRIMITIVE_FIELD_ATTRIBUTES.filter(attribute => field[attribute]);
			if (_.isEmpty(fieldAttributesKeys)) {
				return field.type;
			}
			
			const attributes = fieldAttributesKeys.reduce((attributes, key) => {
				return Object.assign({}, attributes, {
					[key]: field[key]
				});
			}, {});

			return Object.assign({
				type: field.type
			}, attributes);
		}
	});
	return avroSchema;
};

const getMultipleComplexTypeProperties = (schema, type) => {
	const commonComplexFields = ["aliases", "doc", "default"];
	const allowedComplexFields = {
		"enum": [
			"symbols",
			"namespace"
		],
		"fixed": [
			"size",
			"namespace",
			"logicalType"
		],
		"array": ["items"],
		"map": ["values"],
		"record": ["fields"]
	};

	const currentTypeFields = commonComplexFields.concat(allowedComplexFields[type] || []);

	const fieldProperties = currentTypeFields.reduce((fieldProps, prop) => {
		if (schema[prop]) {
			return Object.assign({}, fieldProps, {
				[prop]: schema[prop]
			});
		}

		return fieldProps;
	}, {});

	return fieldProperties;
}

const getFieldWithConvertedType = (schema, field, type, udt) => {
	switch(type) {
		case 'string':
		case 'boolean':
		case 'bytes':
		case 'null':
		case 'array':
			return Object.assign(schema, getField(field, type));
		case 'record':
		case 'enum':
		case 'fixed':
			return Object.assign(schema, getField(field, type), {
				typeName: field.typeName 
			});
		case 'number':
			return Object.assign(schema, getNumberField(field));
		case 'map':
			return Object.assign(schema, {
				type,
				values: getValues(type, field.subtype)
			});
		default:
			const typeFromUdt = getTypeFromUdt(type, udt);
			if (_.isArray(_.get(typeFromUdt, 'type'))) {
				return Object.assign(schema, typeFromUdt, {
					name: schema.name
				} );
			}
			return Object.assign(schema, { type: typeFromUdt || DEFAULT_TYPE });
	}
};

const getTypeFromUdt = (type, udt) => {
	if (!udt[type]) {
		return type;
	}
	const udtItem = cloneUdtItem(udt[type]);
	if (isDefinitionTypeValidForAvroDefinition(udtItem)) {
		delete udt[type];
		if (Array.isArray(udtItem)) {
			return udtItem.map(udtItemType => prepareDefinitionBeforeInsert(udtItemType, udt));
		}
		return prepareDefinitionBeforeInsert(udtItem, udt);
	}

	return udtItem;
};

const isDefinitionTypeValidForAvroDefinition = (definition) => {
	const validTypes = ['record', 'enum', 'fixed', 'array'];
	if (typeof definition === 'string') {
		return validTypes.includes(definition);
	} else if (Array.isArray(definition)) {
		return definition.some(isDefinitionTypeValidForAvroDefinition);
	} else {
		return validTypes.includes(definition.type);
	}
}

const prepareDefinitionBeforeInsert = (definition, udt) => {
	switch(definition.type) {
		case 'record':
			const definitionFields = _.get(definition, 'fields', []);
			const fields = definitionFields.reduce((acc, field) => {
				if (udt[field.type]) {
					const udtItem = cloneUdtItem(udt[field.type]);
					const fieldWithRef = Object.assign({}, field);

					if (isDefinitionTypeValidForAvroDefinition(udtItem)) {
						delete udt[field.type];
					}

					fieldWithRef.type = prepareDefinitionBeforeInsert(udtItem, udt);
					return [...acc, fieldWithRef];
				}
				return [...acc, field];
			}, []);
			return Object.assign({}, definition, { fields });
		case 'array':
			if (udt[definition.items.type]) {
				const udtItem = cloneUdtItem(udt[definition.items.type]);

				if (isDefinitionTypeValidForAvroDefinition(udtItem)) {
					delete udt[definition.items.type];
				}

				return Object.assign({}, definition, { items: { type: udtItem }});
			}
			return Object.assign({}, definition, { items: prepareDefinitionBeforeInsert(definition.items, udt) }); 
		default:
			return definition;
	}
}

const cloneUdtItem = (udt) => {
	if (typeof udt === 'string') {
		return udt;
	} else if (Array.isArray(udt)) {
		return [...udt];
	} else {
		return Object.assign({}, udt);
	}
}

const getTypeFromReference = (schema) => {
	if (!schema.$ref) {
		return;
	}

	const typeName = prepareName(schema.$ref.split('/').pop() || '');

	return typeName;
};

const getValues = (type, subtype) => {
	const regex = new RegExp('\\' + type + '<(.*?)\>');
	return subtype.match(regex)[1] || DEFAULT_TYPE;
};

const handleFields = (schema, avroSchema, udt) => {
	avroSchema.fields = Object.keys(schema.properties).map(key => {
		let field = schema.properties[key];
		let avroField = Object.assign({}, { name: key });
		handleRecursiveSchema(field, avroField, schema, udt);
		return avroField;
	});
};

const handleItems = (schema, avroSchema, udt) => {
	schema.items = !Array.isArray(schema.items) ? [schema.items] : schema.items;
	const schemaItem = schema.items[0] || {};
	const arrayItemType = schemaItem.type || DEFAULT_TYPE;
	const schemaItemName = schemaItem.arrayItemCode || schemaItem.arrayItemName || schemaItem.code || schemaItem.name;

	if (isComplexType(arrayItemType)) {
		avroSchema.items = {};
		handleRecursiveSchema(schemaItem, avroSchema.items, schema, udt);
	} else {
		avroSchema.items = avroSchema.items || {};
		schemaItem.type = schemaItem.type || getTypeFromReference(schemaItem);

		handleType(schemaItem, avroSchema.items, udt);

		if (avroSchema.items.type && typeof avroSchema.items.type === 'object') {
			avroSchema.items = avroSchema.items.type;
		}
	}

	if (schemaItemName) {
		avroSchema.items.name = schemaItemName;
	}
};

const uniqBy = (arr, prop) => {
	return arr.map(function(e) { return e[prop]; }).filter(function(e,i,a){
		return i === a.indexOf(e);
	});
};

const handleOtherProps = (schema, prop, avroSchema) => {
	if (prop === 'default') {
		avroSchema[prop] = getDefault(schema.type, schema[prop]);
	} else if (ADDITIONAL_PROPS.includes(prop)) {
		const allowedProperties = getAllowedPropertyNames(schema.type, schema);
		if (!allowedProperties.includes(prop)) {
			return;
		}
		avroSchema[prop] = schema[prop];

		if (prop === 'size' || prop === 'durationSize') {
			avroSchema[prop] = Number(avroSchema[prop]);
		}
	}
};

const getDefault = (type, value) => {
	const defaultType = _.isArray(type) ? _.first(type) : type;
	if (!_.isString(defaultType)) {
		return value;
	}

	if (defaultType === 'null' && value === 'null') {
		return null;
	}

	return value;
};

const handleComplexTypeStructure = (avroSchema, parentSchema) => {
	const rootComplexProps = ['doc', 'default'];
	const isParentArray = parentSchema && parentSchema.type && parentSchema.type === 'array';
	avroSchema = setDurationSize(avroSchema);

	if (!isParentArray && isComplexType(avroSchema.type)) {
		const name = avroSchema.name;
		const schemaContent = Object.assign({}, avroSchema, { name: avroSchema.typeName || avroSchema.name });

		Object.keys(avroSchema).forEach(function(key) { delete avroSchema[key]; });

		if ((schemaContent.type === 'array' || schemaContent.type === 'map') && name) {
			delete schemaContent.name;
		}
		delete schemaContent.arrayItemName;
		delete schemaContent.typeName;

		avroSchema.name = name;
		avroSchema.type = schemaContent;

		rootComplexProps.forEach(prop => {
			if (schemaContent.hasOwnProperty(prop)) {
				avroSchema[prop] = schemaContent[prop];
				delete schemaContent[prop];
			}
		});
	}
};

const handleSchemaName = (avroSchema, parentSchema) => {
	if (!avroSchema.name && isComplexType(avroSchema.type) && avroSchema.type !== 'array') {
		avroSchema.name = avroSchema.arrayItemName || parentSchema.name || getDefaultName();
	}

	if (avroSchema.name) {
		avroSchema.name = prepareName(avroSchema.name);
	}

	if(avroSchema.type && avroSchema.type.name) {
		avroSchema.type.name = prepareName(avroSchema.type.name);
	}

	delete avroSchema.arrayItemName;
};

const prepareName = (name) => name
	.replace(VALID_FULL_NAME_REGEX, '_')
	.replace(VALID_FIRST_NAME_LETTER_REGEX, '_');

const getDefaultName = () => {
	if (nameIndex) {
		return `${DEFAULT_NAME}_${nameIndex++}`;
	} else {
		nameIndex++;
		return  DEFAULT_NAME;
	}
};

const reorderName = (avroSchema) => {
	let objKeys = Object.keys(avroSchema);
	if (objKeys.includes('name')) {
		objKeys = ['name', ...objKeys.filter(item => item !== 'name')];
	}

	objKeys.forEach(prop => {
		const tempValue = avroSchema[prop];
		delete avroSchema[prop];
		avroSchema[prop] = tempValue;
	});

	return avroSchema;
};

const isComplexType = (type) => {
	if (!type) {
		return false;
	}
	return ['record', 'array', 'fixed', 'enum', 'map'].includes(type);
};

const handleEmptyNestedObjects = (avroSchema) => {
	if (avroSchema.type && avroSchema.type === 'record') {
		avroSchema.fields = (avroSchema.fields) ? avroSchema.fields : [];
	} else if (avroSchema.type && avroSchema.type === 'array') {
		avroSchema.items = (avroSchema.items) ? avroSchema.items : DEFAULT_TYPE;
	}
};

const getTargetFieldLevelPropertyNames = (type, data) => {
	if (!fieldLevelConfig.structure[type]) {
		return [];
	}

	return fieldLevelConfig.structure[type].filter(property => {
		if (typeof property === 'object' && property.isTargetProperty) {
			if (property.dependency) {
				return (data[property.dependency.key] == property.dependency.value);
			} else {
				return true;
			}
		}

		return false;
	}).map(property => property.propertyKeyword);
};

const getAllowedPropertyNames = (type, data) => {
	if (!fieldLevelConfig.structure[type]) {
		return [];
	}

	return fieldLevelConfig.structure[type].filter(property => {
		if (typeof property !== 'object') {
			return true;
		}
		if (!property.dependency) {
			return true;
		}

		return (data[property.dependency.key] === property.dependency.value);
	}).map(property => _.isString(property) ? property : property.propertyKeyword);
};

const handleTargetProperties = (schema, avroSchema) => {
	if (schema.type) {
		const targetProperties = getTargetFieldLevelPropertyNames(schema.type, schema);
		targetProperties.forEach(prop => {
			avroSchema[prop] = schema[prop];
		});
	}
};

const getNumberField = field => {
	const type = field.mode || 'int';

	return getField(field, type);
};

const setDurationSize = field => {
	const size = field.durationSize;
	delete field.durationSize;

	if (field.type !== 'fixed' || field.logicalType !== 'duration' || !size) {
		return field;
	}

	return Object.assign(field, { size });
};

const getField = (field, type) => {
	const logicalType = field.logicalType;
	const correctLogicalTypes = _.get(LOGICAL_TYPES_MAP, type, []);
	const logicalTypeIsCorrect = correctLogicalTypes.includes(logicalType);
	const fieldWithType = Object.assign({}, field, { type });
	let filteredField = {};
	handleTargetProperties(fieldWithType, filteredField);

	if (!logicalTypeIsCorrect) {
		return Object.assign({ type }, filteredField);
	}

	return Object.assign({ type }, filteredField, {
		logicalType
	});
};