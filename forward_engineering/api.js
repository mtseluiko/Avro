'use strict'

const fs = require('fs');
const path = require('path');
const ADDITIONAL_PROPS = ['name', 'arrayItemName', 'doc', 'order', 'aliases', 'symbols', 'namespace', 'size', 'default'];
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
		try {
			const name = getRecordName(data);
			let avroSchema = { name };
			let jsonSchema = JSON.parse(data.jsonSchema);

			handleRecursiveSchema(jsonSchema, avroSchema);

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
			setTimeout(() => {
				return cb({ message: err.message, stack: err.stack });
			}, 150);
		}
	}
};

const getRecordName = (data) => {
	return data.entityData.name || data.entityData.collectionName;
};

const reorderAvroSchema = (avroSchema) => {
	const schemaFields = avroSchema.fields;
	delete avroSchema.fields;
	return Object.assign({}, avroSchema, {
		fields: schemaFields
	});
};

const handleRecursiveSchema = (schema, avroSchema, parentSchema = {}, key) => {
	if (schema.oneOf) {
		handleChoice(schema, 'oneOf');
	}

	if (schema.allOf) {
		handleChoice(schema, 'allOf');
	}

	for (let prop in schema) {
		switch (prop) {
			case 'type':
				handleType(schema, avroSchema);
				break;
			case 'properties':
				handleFields(schema, avroSchema);
				break;
			case 'items':
				handleItems(schema, avroSchema);
				break;
			default:
				handleOtherProps(schema, prop, avroSchema);
		}
	}
	handleComplexTypeStructure(avroSchema, parentSchema);
	handleSchemaName(avroSchema, parentSchema);
	avroSchema = reorderName(avroSchema);
	handleEmptyNestedObjects(avroSchema);
	handleTargetProperties(schema, avroSchema);
	return;
};

const handleChoice = (schema, choice) => {
	let allSubSchemaFields = [];
	const choiceMeta = schema[`${choice}_meta`] ? schema[`${choice}_meta`].name : null;

	schema[choice].forEach(subSchema => {
		if (subSchema.oneOf) {
			handleChoice(subSchema, 'oneOf');
		}
		allSubSchemaFields = allSubSchemaFields.concat(Object.keys(subSchema.properties).map(item => {
			return Object.assign({
				name: item
			}, subSchema.properties[item]);
		}));
	});

	let multipleFieldsHash = {};
	allSubSchemaFields.forEach(field => {
		if (!multipleFieldsHash[choiceMeta || field.name]) {
			multipleFieldsHash[choiceMeta || field.name] = {
				name: choiceMeta || field.name,
				type: []
			};
		}
		let multipleField = multipleFieldsHash[choiceMeta || field.name];
		const filedType = field.type;

		if (isComplexType(filedType)) {
			let newField = {};
			handleRecursiveSchema(field, newField);
			multipleField.type.push(newField);
		} else if (Array.isArray(filedType)) {
			multipleField.type = multipleField.type.concat(filedType);
		} else {
			multipleField.type = multipleField.type.concat([filedType]);
		}
	});

	schema.properties = Object.assign((schema.properties || {}), multipleFieldsHash);
};

const handleType = (schema, avroSchema) => {
	if (Array.isArray(schema.type)) {
		avroSchema = handleMultiple(avroSchema, schema, 'type');
	} else {
		avroSchema = getFieldWithConvertedType(avroSchema, schema, schema.type);
	}
};

const handleMultiple = (avroSchema, schema, prop) => {
	avroSchema[prop] = schema[prop].map(type => {
		if (type && typeof type === 'object') {
			return type.type;
		} else {
			const field = getFieldWithConvertedType({}, schema, type);
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

	const currentTypeFields = commonComplexFields.concat(allowedComplexFields[type]);

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

const getFieldWithConvertedType = (schema, field, type) => {
	switch(type) {
		case 'string':
		case 'bytes':
		case 'boolean':
		case 'null':
		case 'array':
			return Object.assign(schema, { type });
		case 'record':
		case 'enum':
		case 'fixed':
			return Object.assign(schema, { 
				type,
				typeName: field.typeName 
			});
		case 'number':
			return Object.assign(schema, { type: getNumberType(field) });
		case 'map':
			return Object.assign(schema, {
				type,
				values: getValues(type, field.subtype)
			});
		default:
			return Object.assign(schema, { type: DEFAULT_TYPE });
	}
};

const getValues = (type, subtype) => {
	const regex = new RegExp('\\' + type + '<(.*?)\>');
	return subtype.match(regex)[1] || DEFAULT_TYPE;
};

const handleFields = (schema, avroSchema) => {
	avroSchema.fields = Object.keys(schema.properties).map(key => {
		let field = schema.properties[key];
		let avroField = Object.assign({}, { name: key });
		handleRecursiveSchema(field, avroField, schema);
		return avroField;
	});
};

const handleItems = (schema, avroSchema) => {
	schema.items = !Array.isArray(schema.items) ? [schema.items] : schema.items;

	const arrayItemType = schema.items[0].type || DEFAULT_TYPE;
	if (isComplexType(arrayItemType)) {
		avroSchema.items = {};
		handleRecursiveSchema(schema.items[0], avroSchema.items, schema);
	} else {
		avroSchema.items = getFieldWithConvertedType({}, schema.items[0], arrayItemType).type;
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
		avroSchema.name = avroSchema.name.replace(VALID_FULL_NAME_REGEX, '_')
			.replace(VALID_FIRST_NAME_LETTER_REGEX, '_');
	}

	if(avroSchema.type && avroSchema.type.name) {
		avroSchema.type.name = avroSchema.type.name.replace(VALID_FULL_NAME_REGEX, '_')
			.replace(VALID_FIRST_NAME_LETTER_REGEX, '_');
	}

	delete avroSchema.arrayItemName;
};

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
		return type;
	}
};