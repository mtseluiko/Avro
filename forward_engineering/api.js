'use strict'

const ADDITIONAL_PROPS = ['name', 'doc', 'order', 'aliases', 'symbols', 'namespace', 'size', 'default'];
const DEFAULT_NAME = 'new_name';

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
            return cb(null, avroSchema);
        } catch(err) {
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
				handleItems(schema, prop, avroSchema, key);
				break;
			default:
				handleOtherProps(schema, prop, avroSchema);
		}
    }

    handleComplexTypeStructure(avroSchema, parentSchema);
    handleSchemaName(avroSchema, parentSchema, key);
    
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
    const regex = new RegExp('\\' + type + '<(.*?)\>');
    return subtype.match(regex)[1] || 'string';
};

const handleFields = (schema, prop, avroSchema) => {
	avroSchema.fields = Object.keys(schema[prop]).map(key => {
        let field = schema[prop][key];
        let avroField = Object.assign({}, { name: key });
        handleRecursiveSchema(field, avroField, schema, key);
        return avroField;
	});
};

const handleItems = (schema, prop, avroSchema, key) => {
    if (!Array.isArray(schema[prop])) {
        schema[prop] = [schema[prop]];
    }

    avroSchema[prop] = {};
    handleRecursiveSchema(schema[prop][0], avroSchema[prop], schema, key);
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

        [...ADDITIONAL_PROPS, 'mode', 'subtype'].forEach(prop => {
            if (field[prop]) {
                multipleField[prop] = field[prop];
            }
        });
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
        const schemaContent = Object.assign({}, avroSchema);
       
        Object.keys(avroSchema).forEach(function(key) { delete avroSchema[key]; });
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

const handleSchemaName = (avroSchema, parentSchema, key) => {
    if (!avroSchema.name && isComplexType(avroSchema.type)) {
        avroSchema.name = avroSchema.name || parentSchema.name || key || DEFAULT_NAME;
    }
};

const isComplexType = (type) => {
    return ['record', 'array', 'fixed', 'enum'].includes(type);
};