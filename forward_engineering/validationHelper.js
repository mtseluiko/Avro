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
        const errors = (err instanceof TypeError) ? avsc.errorsCollector : avsc.errorsCollector.concat(err);
		const errorMessages = errors.map(toMessage);

        avsc.errorsCollector = [];

        return errorMessages;
    }
};

module.exports = {
	validate
};
