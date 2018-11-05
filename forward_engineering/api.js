'use strict'

const ADDITIONAL_PROPS = ['name', 'arrayItemName', 'doc', 'order', 'aliases', 'symbols', 'namespace', 'size', 'default'];
const DEFAULT_TYPE = 'string';
const DEFAULT_NAME = 'New_field';
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
        handleOneOf(schema);
    }

    for (let prop in schema) {
		switch(prop) {
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
    handleEmptyNestedObjects(avroSchema);
	return;
};

const handleType = (schema, avroSchema) => {
    if (Array.isArray(schema.type)) {
        avroSchema = handleMultiple(avroSchema, schema, 'type');
    } else {
        avroSchema = getFieldWithConvertedType(avroSchema, schema, schema.type);
    }
};

const handleMultiple = (avroSchema, schema, prop) => {
    avroSchema[prop] = schema[prop].map(item => {
        if (item && typeof item === 'object') {
            return item.type;
        } else {
            const field = getFieldWithConvertedType({}, schema, item);
            return field.type;
        }
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

const handleOneOf = (schema) => {
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
        const filedType = field.type;

        if (isComplexType(filedType)) {
            let newField =  {};
            handleRecursiveSchema(field, newField);
            multipleField.type.push(newField);
            //additional props
        } else if (Array.isArray(filedType)) {
            multipleField.type = multipleField.type.concat(filedType);
        } else {
            multipleField.type = multipleField.type.concat([filedType]);
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

        if ((schemaContent.type === 'array' || schemaContent.type === 'map') && name) {
            delete schemaContent.name;
        }

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
        delete avroSchema.arrayItemName;
    }
};

const getDefaultName = () => {
    if (nameIndex) {
        return `${DEFAULT_NAME}_${nameIndex++}`;
    } else {
        nameIndex++;
        return  DEFAULT_NAME;
    }
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