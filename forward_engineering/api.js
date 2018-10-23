'use strict'

const ADDITIONAL_PROPS = ['name', 'doc'];

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

const generateScript = (data, logger, cb) => {
    let avroSchema = { name: data.name };
    handleRecursiveSchema(data.schema, avroSchema);
    avroSchema.type = 'record';
    return cb(null, avroSchema);
};

const handleRecursiveSchema = (schema, avroSchema, parentSchema = {}) => {
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
    avroSchema = getType(avroSchema, schema, schema[prop]);
};

const getType = (schema, field, type) => {
	switch(type) {
		case 'string':
		case 'bytes':
		case 'number':
		case 'boolean':
		case 'null':
		case 'record':
		case 'array':
		case 'enum':
		case 'fixed':
			return Object.assign(schema, { type });
		case 'map':
			return Object.assign(schema, {
				type,
				values: field.subtype
			});
		default:
			return Object.assign(schema, { type: 'string' });
	}
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

    avroSchema[prop] = Object.assign({}, schema[prop][0]);
    handleRecursiveSchema(schema[prop][0], avroSchema[prop], schema);
};

const handleOtherProps = (schema, prop, avroSchema) => {
    if (ADDITIONAL_PROPS.includes(prop)) {
        avroSchema[prop] = schema[prop];
    }
};


/*

const data = {
    jsonSchema: { 
        "$schema": "http://json-schema.org/draft-04/schema#",
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "New field": {
                "type": "string"
            },
            "New field(1)": {
                "type": "bytes"
            },
            "New field(2)": {
                "type": "number"
            },
            "New field(3)": {
                "type": "boolean"
            },
            "New field(4)": {
                "type": "null"
            },
            "New field(5)": {
                "type": "record",
                "additionalProperties": false
            },
            "New field(6)": {
                "type": "array",
                "additionalItems": true,
                "uniqueItems": false,
                "required": [
                    null
                ],
                "items": {
                    "type": "string",
                    "arrayItem": true
                }
            },
            "New field(7)": {
                "type": "map",
                "subtype": "map<string>",
                "keyType": "string",
                "additionalProperties": false
            },
            "New field(8)": {
                "type": "enum",
                "pattern": "[A-Za-z0-9_]"
            },
            "New field(9)": {
                "type": "fixed"
            }
        },
        "required": [
            "New field",
            "New field(1)",
            "New field(2)",
            "New field(3)",
            "New field(5)",
            "New field(6)",
            "New field(7)",
            "New field(8)",
            "New field(9)"
        ]
    },
    name: 'AllTypes'
};

generateScript(data, {}, (err, res) => {
    console.log(err, res);
});

*/