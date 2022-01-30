var jsonata = require("jsonata");
const { Transform } = require('stream');

class JsonataTransform extends Transform {
    constructor(expression) {
        super({
            readableObjectMode: true,
            writableObjectMode: true
        })
        this.expression = expression;
        this.expressionEvaluated =  jsonata(expression);
    }

    _transform(chunk, encoding, next) {
        let expResult = this.expressionEvaluated.evaluate(chunk.value);
        if(expResult)
            return next(null, chunk);
        next();
    }
}

module.exports = JsonataTransform;
