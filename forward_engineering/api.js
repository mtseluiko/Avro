'use strict'

const fs = require('fs');
const path = require('path');
const validationHelper = require('./validationHelper');

const ADDITIONAL_PROPS = ['doc', 'order', 'aliases', 'symbols', 'namespace', 'size', 'default', 'pattern'];
const DEFAULT_TYPE = 'string';
const DEFAULT_NAME = 'New_field';
const VALID_FULL_NAME_REGEX = /[^A-Za-z0-9_]/g;
const VALID_FIRST_NAME_LETTER_REGEX = /^[0-9]/;
const readConfig = (pathToConfig) => {
	return JSON.parse(fs.readFileSync(path.join(__dirname, pathToConfig)).toString().replace(/\/\*[.\s\S]*?\*\//ig, ""));
};
const fieldLevelConfig = readConfig('../properties_pane/field_level/fieldLevelConfig.json');
let nameIndex = 0;

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
			cb(e.message);
		}
	}
};

const getUserDefinedTypes = ({ internalDefinitions, externalDefinitions, modelDefinitions }) => {
	let udt = convertSchemaToUserDefinedTypes(JSON.parse(externalDefinitions), {});
	 udt = convertSchemaToUserDefinedTypes(JSON.parse(modelDefinitions), udt);
	 udt = convertSchemaToUserDefinedTypes(JSON.parse(internalDefinitions), udt);

	return udt;
};

const convertSchemaToUserDefinedTypes = (jsonSchema, udt) => {
	const avroSchema = {};

	handleRecursiveSchema(jsonSchema, avroSchema, {}, udt);

	return (avroSchema.fields || []).reduce((result, field) => {
		return Object.assign({}, result, {
			[field.name]: field.type
		});
	}, udt);
};

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
	handleNull(schema, avroSchema);
	return;
};

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

	let choiceMeta = {};
	let allSubSchemaFields = [];
	
	if (choiceRawMeta) {
		choiceMeta = Object.keys(choiceRawMeta).reduce((choiceMeta, prop) => {
			if (ADDITIONAL_PROPS.includes(prop) && typeof choiceRawMeta[prop] !== "undefined") {
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
			if (choiceMeta.default) {
				choiceMeta.default = convertDefaultMetaFieldType(field.type, choiceMeta.default);
			}
			
			multipleFieldsHash[fieldName] = Object.assign({}, field.choiceMeta, {
				name: fieldName,
				type: [],
				choiceMeta
			});
		}
		let multipleField = multipleFieldsHash[fieldName];
		const filedType = field.type || getTypeFromReference(field) || DEFAULT_TYPE;

		multipleField.nullAllowed = multipleField.nullAllowed || field.nullAllowed;
		field = Object.assign({}, field, { nullAllowed: false });

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
	});

	schema.properties = Object.assign((schema.properties || {}), multipleFieldsHash);
};

const handleNull = (jsonSchema, avroSchema) => {
	if (!jsonSchema.nullAllowed) {
		return avroSchema;
	}

	if (Array.isArray(avroSchema.type)) {
		if (!avroSchema.type.includes('null')) {
			avroSchema.type.unshift('null');
		}
	} else if (avroSchema.type !== 'null') {
		avroSchema.type = ['null', avroSchema.type];
	}

	if (avroSchema.default === 'null') {
		avroSchema.default = null;
	}

	return avroSchema;
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

				return Object.assign({}, fieldProperties, {
					name: fieldName,
					type
				})
			}

			return field.type;
		}
	});
	return avroSchema;
};

const getMultipleComplexTypeProperties = (schema, type) => {
	const commonComplexFields = ["aliases", "doc", "default"];
	const allowedComplexFields = {
		"enum": [
			"symbols",
			"pattern",
			"namespace"
		],
		"fixed": [
			"size",
			"namespace"
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
		case 'bytes':
		case 'boolean':
		case 'null':
		case 'array':
			return Object.assign(schema, { type, });
		case 'record':
		case 'enum':
		case 'fixed':
			return Object.assign(schema, { 
				type,
				typeName: field.typeName 
			});
		case 'number':
			return Object.assign(schema, getNumberType(field));
		case 'map':
			return Object.assign(schema, {
				type,
				values: getValues(type, field.subtype)
			});
		default:
			return Object.assign(schema, { type: getTypeFromUdt(type, udt) || DEFAULT_TYPE });
	}
};

const getTypeFromUdt = (type, udt) => {
	if (!udt[type]) {
		return type;
	}
	const udtType = udt[type];
	delete udt[type];

	return udtType;
};

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
	if (ADDITIONAL_PROPS.includes(prop)) {
		avroSchema[prop] = schema[prop];

		if (prop === 'size') {
			avroSchema[prop] = Number(avroSchema[prop]);
		}
	}
};

const handleComplexTypeStructure = (avroSchema, parentSchema) => {
	const rootComplexProps = ['doc', 'default'];
	const isParentArray = parentSchema && parentSchema.type && parentSchema.type === 'array';

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

const handleTargetProperties = (schema, avroSchema) => {
	if (schema.type) {
		const targetProperties = getTargetFieldLevelPropertyNames(schema.type, schema);
		targetProperties.forEach(prop => {
			avroSchema[prop] = schema[prop];
		});
	}
};

const getNumberType = (field) => {
	const type = field.mode || 'int';

	if (field.logicalType) {
		return {
			type: type,
			logicalType: field.logicalType
		};
	} else {
		return {
			type
		};
	}
};