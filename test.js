let fs = require('fs')
let assert = require('assert')
let {
    createRotatable
} = require('./index')

function nextWrite(data, streams, callBack, expectedResults) {

    if (!expectedResults) {
        expectedResults = {}
        streams.forEach(stream => {
            expectedResults[stream.name] = {
                writeCount: 0,
                written: 0
            }
        })
    }

    if (!data.length) {
        streams.forEach(stream => {
            stream.instance.end()
        })
        callBack(expectedResults)
    } else {

        let d = data.pop()
        let writes = 0

        streams.forEach(stream => {
            let name = stream.name
            d.w = name
            let body = JSON.stringify(d) + '\n'
            stream.instance.write(body, function () {
                expectedResults[name].written += body.length
                if (++writes === streams.length) {
                    ++expectedResults[name].writeCount
                    nextWrite(data, streams, callBack, expectedResults)
                }
            })
        })
    }
}

function cleanup() {
    fs.readdirSync('.')
        .filter(function (path) {
            return path.indexOf('test.log') === 0
        })
        .forEach(function (path) {
            fs.unlinkSync(path)
        })
}

function readFiles() {
    return fs.readdirSync('.')
        .filter(function (path) {
            return path.indexOf('test.log') === 0
        })
        .reduce(function (files, path) {
            files.push({
                path: path,
                body: fs.readFileSync(path).toString(),
                stats: fs.statSync(path)
            })
            return files
        }, [])
}

describe('rotatable tests', function () {

    describe('prior version tests', function () {
        let results = {
            written: 0,
            rotates: []
        }

        let data = [
            { i: 1 },
            { i: 2 },
            { i: 3 },
            { i: 4 },
            { i: 5 },
            { i: 6 }
        ]

        it('rotates at ~100B', function () {
            results.files.forEach(function (f) {
                assert(f.stats.size < 105,
                    'file size is smaller than 105 bytes')

                if (f.path !== 'test.log') {
                    assert(f.stats.size > 100,
                        'rotated file size is greater than 100 bytes')
                }
            })
        })

        it('contains all of the data', function () {
            var out = results.files.map(function (f) {
                return f.body
            }).join('\n')

            var objs = out.split('\n')
                .filter(function (line) {
                    return !!line
                })
                .map(JSON.parse)

            data.forEach(function (d) {
                var w1found = objs.filter(function (obj) {
                    return obj.w === 'w1' && obj.i === d.i
                })

                var w2found = objs.filter(function (obj) {
                    return obj.w === 'w1' && obj.i === d.i
                })

                assert.equal(w1found.length, 1)
                assert.equal(w2found.length, 1)
            })
        })

        it('emits rotate events', function () {
            assert.equal(results.rotates.length, 1)

            var file = results.rotates[0]
            assert.equal(file.indexOf('test.log'), 0)
        })

        before(function (done) {
            cleanup()

            // create two parallel writers
            var w1 = createRotatable('test.log', {
                size: 100,
                suffix: '.1'
            })
                .on('finish', finish)
                .on('rotate', function (file) {
                    results.rotates.push(file)
                })
            var w2 = createRotatable('test.log', {
                size: 100,
                suffix: '.2'
            })
                .on('finish', finish)
                .on('rotate', function (file) {
                    results.rotates.push(file)
                })

            next()

            function next(err) {
                var d = data.pop()
                if (!d) {
                    w1.end()
                    w2.end()
                    return
                }

                var saved = 0
                var body
                d.w = 'w1'
                body = JSON.stringify(d) + '\n'
                results.written += body.length
                w1.write(body, function () {
                    if (++saved === 2) {
                        next()
                    }
                })

                d.w = 'w2'
                body = JSON.stringify(d) + '\n'
                results.written += body.length
                w2.write(body, function () {
                    if (++saved === 2) {
                        next()
                    }
                })
            }

            var finished = 0

            function finish() {
                if (++finished === 2) {
                    results.files = readFiles()
                    done()
                }
            }

        })

        after(cleanup)

    })

    describe('General tests', function () {

        before(function () { // runs before all tests in this block
            cleanup()
        })

        after(function () { // runs after all tests in this block
            cleanup()
        })

        beforeEach(function () {
            cleanup()
            this.data = [
                { i: 1 },
                { i: 2 },
                { i: 3 },
                { i: 4 },
                { i: 5 },
                { i: 6 }
            ]
        })

        afterEach(function () {
            cleanup()
        })

        it('All data is written', function (done) {
            let written = 0
            let path = 'test.log'
            let w1 = createRotatable(path, {
                size: '1MB',
                suffix: '.something'
            })
            let body
            let writes = 0
            let expectedEndBody = ''
            let expectedWrites = this.data.length
            do {
                let d = this.data.pop()
                d.w = 'w1'
                body = JSON.stringify(d) + '\n'
                expectedEndBody += body
                w1.write(body, () => {
                    written += body.length
                    if (++writes === expectedWrites) {
                        w1.end()
                        runChecks()
                    }
                })
            } while (this.data.length)

            function runChecks() {
                let files = readFiles()
                let out = files.map(function (f) {
                    return f.body
                }).join('\n')

                assert.equal(expectedWrites, writes)
                assert.equal(files.length, 1)
                assert.equal(files[0]['body'], expectedEndBody)
                assert.equal(out.length, written)
                done()
            }
        })

        it('Emitts rotate event', function (done) {
            let path = 'test.log'
            let rotateCount = 0

            let w1 = createRotatable(path, {
                size: 10,
                suffix: '.something'
            })
                .on('rotate', () => {
                    rotateCount++
                })

            var streams = [{
                name: 'w1',
                instance: w1
            }]

            nextWrite(this.data, streams, runChecks)

            function runChecks(expectedResults) {
                let files = readFiles()
                let actualData = files.map(function (f) {
                    return f.body
                }).join('')

                let writtenDataLength = streams.map((s) => {
                    return expectedResults[s.name].written
                }).reduce((a, b) => a + b, 0)

                assert.equal(actualData.length, writtenDataLength)
                assert.notEqual(rotateCount, 0)
                done()
            }
        })

        it('Emitts compress event', function (done) {
            let path = 'test.log'
            let compressCount = 0

            let w1 = createRotatable(path, {
                size: 10,
                suffix: '.something',
                gzip: true
            })
                .on('compress', () => {
                    compressCount++
                })

            var streams = [{
                name: 'w1',
                instance: w1
            }]

            nextWrite(this.data, streams, runChecks)

            function runChecks() {
                let files = readFiles()
                assert.notEqual(compressCount, files.length)
                assert.notEqual(compressCount, 0)
                done()
            }
        })

        it('Two streams rotating on the same file', function (done) {
            let path = 'test.log'
            let rotateCount = 0

            let w1 = createRotatable(path, {
                size: 100,
                suffix: '.something'
            })
                .on('rotate', () => {
                    rotateCount++
                })

            let w2 = createRotatable(path, {
                size: 100,
                suffix: '.something'
            })
                .on('rotate', () => {
                    rotateCount++
                })
            let streams = [{
                name: 'w1',
                instance: w1
            },
            {
                name: 'w2',
                instance: w2
            }
            ]
            nextWrite(this.data, streams, runChecks)

            function runChecks(expectedResults) {
                let files = readFiles()


                let actualData = files.map(function (f) {
                    return f.body
                }).join('')

                let writtenDataLength = streams.map((s) => {
                    return expectedResults[s.name].written
                }).reduce((a, b) => a + b, 0)

                assert.equal(actualData.length, writtenDataLength)

                assert.equal(rotateCount, 1)
                done()
            }
        })

        it('check rotation WO/compression', function (done) {
            let path = 'test.log'
            let rotateCount = 0

            let w1 = createRotatable(path, {
                size: 10,
                suffix: '.something'
            })
                .on('rotate', () => {
                    rotateCount++
                })

            var streams = [{
                name: 'w1',
                instance: w1
            }]

            nextWrite(this.data, streams, runChecks)

            function runChecks(expectedResults) {
                let files = readFiles()
                let actualData = files.map(function (f) {
                    return f.body
                }).join('')

                let writtenDataLength = streams.map((s) => {
                    return expectedResults[s.name].written
                }).reduce((a, b) => a + b, 0)

                assert.equal(actualData.length, writtenDataLength)
                assert.notEqual(rotateCount, 0)
                done()
            }
        })

        it('check rotation W/compression', function (done) {
            let path = 'test.log'
            let compressCount = 0
            let rotateCount = 0

            let w1 = createRotatable(path, {
                size: 10,
                suffix: '.something',
                gzip: true
            })
                .on('compress', () => {
                    compressCount++
                })
                .on('rotate', () => {
                    rotateCount++
                })

            nextWrite(
                this.data, [{
                    name: 'w1',
                    instance: w1
                }],
                runChecks)

            function runChecks() {
                let files = readFiles()
                assert.notEqual(compressCount, files.length)
                assert.notEqual(compressCount, 0)
                assert.notEqual(rotateCount, 0)
                done()
            }
        })

        it('File exists', function (done) {

            let path = 'test.log'
            fs.openSync(path, 'a')
            let w1 = createRotatable(path, {
                size: 1000,
                suffix: '.something',
                gzip: true
            })
            let streams = [{
                name: 'w1',
                instance: w1
            }]

            nextWrite(this.data, streams, runChecks)

            function runChecks(expectedResults) {
                let files = readFiles()
                let actualData = files.map(function (f) {
                    return f.body
                }).join('')

                let writtenDataLength = streams.map((s) => {
                    return expectedResults[s.name].written
                }).reduce((a, b) => a + b, 0)

                assert.equal(actualData.length, writtenDataLength)
                assert.equal(files.length, 1)
                done()
            }
        })

        it('Opening not in append mode (not ‘a’ flag)', function (done) {
            let path = 'test.log'

            assert.throws(
                function () {
                    createRotatable(path, {
                        flags: 'w',
                        size: 1000,
                        suffix: '.something',
                        gzip: true
                    })
                },
                /RotateStream must be opened in append mode/,
                'unexpected error'
            )
            done()
        })

        it('Numeric size', function (done) {
            let path = 'test.log'
            let options = {
                size: 1000,
                suffix: '.something',
                gzip: true
            }
            let w1 = createRotatable(path, options)
            let streams = [{
                name: 'w1',
                instance: w1
            }]

            nextWrite(this.data, streams, runChecks)

            function runChecks(expectedResults) {
                let files = readFiles()
                let actualData = files.map(function (f) {
                    return f.body
                }).join('')

                let writtenDataLength = streams.map((s) => {
                    return expectedResults[s.name].written
                }).reduce((a, b) => a + b, 0)

                assert.equal(actualData.length, writtenDataLength)
                assert.equal(files.length, 1)
                done()
            }
        })

        it('String size', function (done) {
            let path = 'test.log'
            let w1 = createRotatable(path, {
                size: '10mb',
                suffix: '.something',
                gzip: true
            })
            let streams = [{
                name: 'w1',
                instance: w1
            }]

            nextWrite(this.data, streams, runChecks)

            function runChecks(expectedResults) {
                let files = readFiles()
                let actualData = files.map(function (f) {
                    return f.body
                }).join('')

                let writtenDataLength = streams.map((s) => {
                    return expectedResults[s.name].written
                }).reduce((a, b) => a + b, 0)

                assert.equal(actualData.length, writtenDataLength)
                assert.equal(files.length, 1)
                done()
            }
        })

        it('No suffix', function (done) {
            let path = 'test.log'
            let w1 = createRotatable(path, {
                size: 1000,
                gzip: true
            })
            let streams = [{
                name: 'w1',
                instance: w1
            }]

            nextWrite(this.data, streams, runChecks)

            function runChecks(expectedResults) {
                let files = readFiles()
                let actualData = files.map(function (f) {
                    return f.body
                }).join('')

                let writtenDataLength = streams.map((s) => {
                    return expectedResults[s.name].written
                }).reduce((a, b) => a + b, 0)

                assert.equal(actualData.length, writtenDataLength)
                assert.equal(files.length, 1)
                done()
            }
        })

        it('No aws', function (done) {
            let path = 'test.log'

            assert.throws(
                function () {
                    createRotatable(path, {
                        upload: 'not a falsy value',
                        size: 1000,
                        gzip: true
                    })
                },
                /The aws-sdk module isn't installed/,
                'unexpected error'
            )
            done()
        })
    })
})
