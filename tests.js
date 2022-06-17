var assert = require('assert');
const {validateNumber, loadSourceDatabase } = require('./index');


describe('#ValidateNumber()', function () {
    it('should say "Valid number: 2" and return true passing "2"', function () {
        var r = validateNumber(2);
        assert.equal(r, true);
    });

    it('should should say "Please, enter a valid int number" and return false passing "test"', function () {
        var r = validateNumber("test");
        assert.equal(r, false);
    });

    it('should should say "Please, enter a valid int number" and return false passing "2.5"', function () {
        var r = validateNumber(2.5);
        assert.equal(r, false);
    });
});
describe('#loadSourceDatabase()', function () {
    it('should add 2 docs to the Source Database', async function () {
        var r = await loadSourceDatabase(2);
        assert.equal(r, 2);
    });
});