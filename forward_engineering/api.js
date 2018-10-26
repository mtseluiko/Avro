'use strict'

const ADDITIONAL_PROPS = ['name', 'doc', 'order', 'aliases', 'symbols'];

module.exports = {
	generateScript(data, logger, cb) {
        let name = getRecordName(data);
        let avroSchema = { name };
        let jsonSchema = JSON.parse(data.jsonSchema);

        handleRecursiveSchema(jsonSchema, avroSchema);
        avroSchema.type = 'record';
        avroSchema = JSON.stringify(avroSchema, null, 4);
        return cb(null, avroSchema);
	}
};

const getRecordName = (data) => {
    return data.entityData.name || data.entityData.collectionName;
};

const handleRecursiveSchema = (schema, avroSchema, parentSchema = {}) => {
    if (schema.oneOf) {
        handleOneOf(schema, avroSchema);
    }

    for (let prop in schema) {
		switch(prop) {
			case 'type':
				handleType(schema, prop, avroSchema, parentSchema);
				break;
			case 'properties':
				handleFields(schema, prop, avroSchema);
				break;
			case 'items':
				handleItems(schema, prop, avroSchema);
				break;
			default:
				handleOtherProps(schema, prop, avroSchema);
		}
    }

    
	return;
};

const handleType = (schema, prop, avroSchema, parentSchema) => {
    if (Array.isArray(schema[prop])) {
        avroSchema = handleMultiple(avroSchema, schema, prop);
    } else {
        avroSchema = getFieldWithConvertedType(avroSchema, schema, schema[prop]);
    }
};

const handleMultiple = (avroSchema, schema, prop) => {
    avroSchema[prop] = schema[prop].map(type => {
        const field = getFieldWithConvertedType({}, schema, type);
        return field.type;
    });
    return avroSchema;
};

const getFieldWithConvertedType = (schema, field, type) => {
	switch(type) {
		case 'string':
		case 'bytes':
		case 'boolean':
		case 'null':
		case 'record':
		case 'array':
		case 'enum':
		case 'fixed':
            return Object.assign(schema, { type });
        case 'number':
            return Object.assign(schema, { type:  field.mode || 'int' });
		case 'map':
			return Object.assign(schema, {
				type,
				values: getValues(type, field.subtype)
			});
		default:
			return Object.assign(schema, { type: 'string' });
	}
};

const getValues = (type, subtype) => {
    // const regex = new RegExp('\\' + type + '<(.*?)\>');
    // return subtype.match(regex)[1] || 'string';
    return subtype;
};

const handleFields = (schema, prop, avroSchema) => {
	avroSchema.fields = Object.keys(schema[prop]).map(key => {
        let field = schema[prop][key];
        let avroField = Object.assign({}, { name: key });
        handleRecursiveSchema(field, avroField, schema);
        return avroField;
	});
};

const handleItems = (schema, prop, avroSchema) => {
    if (!Array.isArray(schema[prop])) {
        schema[prop] = [schema[prop]];
    }

    avroSchema[prop] = {};
    handleRecursiveSchema(schema[prop][0], avroSchema[prop], schema);
};

const handleOneOf = (schema, avroSchema) => {
    let allSubSchemaFields = [];
    schema.oneOf.forEach(subSchema => {
        allSubSchemaFields = allSubSchemaFields.concat(Object.keys(subSchema.properties).map(item => {
            return Object.assign({
                name: item
            }, subSchema.properties[item]);
        }));
    });
    const sharedFieldNames = uniqBy(allSubSchemaFields, 'name');
    const commonFields = allSubSchemaFields.filter(item => sharedFieldNames.includes(item.name));
    
    let multipleFieldsHash = {};
    commonFields.forEach(field => {
        if (!multipleFieldsHash[field.name]) {
            multipleFieldsHash[field.name] = {
                name: field.name,
                type: []
            };
        }
        let multipleField = multipleFieldsHash[field.name];
        let fieldTypes = (Array.isArray(field.type) ? field.type : [field.type]);
        multipleField.type = multipleField.type.concat(fieldTypes);

        if (field.properties) {
            multipleField.properties = Object.assign((multipleField.properties || {}), field.properties);
        }

        if (field.items) {
            multipleField.items = Object.assign((multipleField.items || {}), field.items);
        }
    });

    schema.properties = Object.assign((schema.properties || {}), multipleFieldsHash);
};

const uniqBy = (arr, prop) => {
    return arr.map(function(e) { return e[prop]; }).filter(function(e,i,a){
        return i === a.indexOf(e);
    });
};

const handleOtherProps = (schema, prop, avroSchema) => {
    if (ADDITIONAL_PROPS.includes(prop)) {
        avroSchema[prop] = schema[prop];
    }
};

const generateScript = (data, logger, cb) => {
    let avroSchema = { name: data.name };
    handleRecursiveSchema(data.jsonSchema, avroSchema);
    avroSchema.type = 'record';
    return cb(null, avroSchema);
};

const data = {
    jsonSchema: {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "type": "object",
        "additionalProperties": true,
        "properties": {
            "New field": {
                "type": [
                    "null",
                    "number"
                ],
                "mode": "long"
            }
        },
        "oneOf": [
            {
                "type": "object",
                "properties": {
                    "New field1": {
                        "type": "number"
                    }
                },
                "additionalProperties": true,
                "required": [
                    "New field"
                ]
            },
            {
                "type": "object",
                "properties": {
                    "New field1": {
                        "type": "map",
                        "subtype": "string",
                        "additionalProperties": false
                    }
                },
                "additionalProperties": false,
                "required": [
                    "New field"
                ]
            }
        ],
        "required": [
            "New field"
        ]
    },
    name: 'Multi'
};

generateScript(data, {}, (err, res) => {
    console.log(err, res);
});
