const avsc = require('./modules/avsc');

const toMessage = (err) => ({
    type: 'error',
    label: err.fieldName || err.name,
    title: err.message,
    context: ''
});

const validate = (script) => {
    try {
        avsc.parse(script);
        
        if (avsc.errorsCollector && avsc.errorsCollector.length) {
            const messages = avsc.errorsCollector.map(toMessage);
            avsc.errorsCollector = [];

            return messages;
        } else {
            return [{
                type: 'success',
                label: '',
                title: 'Avro schema is valid',
                context: ''
            }];
        }
    } catch(err) {
        return [{
            type: 'error',
            label: err.fieldName || err.name,
            title: err.message,
            context: ''
        }];
    }
};

module.exports = {
	validate
};
