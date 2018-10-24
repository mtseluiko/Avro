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
        const field = getFieldWithConvertedType({}, {}, type);
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
            return Object.assign(schema, { type:  field.mode });
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
    const regex = new RegExp('\\' + type + '<(.*?)\>');
    return subtype.match(regex)[1] || 'string';
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

const handleOtherProps = (schema, prop, avroSchema) => {
    if (ADDITIONAL_PROPS.includes(prop)) {
        avroSchema[prop] = schema[prop];
    }
};