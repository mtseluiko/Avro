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
        if (err instanceof TypeError) {
            return avsc.errorsCollector || [];
        }

        const errors = avsc.errorsCollector.concat([{
            type: 'error',
            label: err.fieldName || err.name,
            title: err.message,
            context: ''
        }]);

        avsc.errorsCollector = [];

        return errors;
    }
};

module.exports = {
	validate
};
