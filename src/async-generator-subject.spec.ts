import { AsyncGeneratorSubject } from "./async-generator-subject"
import chaiAsPromised from "chai-as-promised"
import chai, { expect } from "chai"

chai.use(chaiAsPromised)
chai.should()

describe("async generator subject consumed by cursor", () => {

    it("normal behaviour", (done) => {
        //state
        let subscribed: boolean = true
        let onUnscubscribe: () => void
        let reject: (error: Error) => void
        let unscubscribedPromise = new Promise((resolve, rej) => {
            onUnscubscribe = resolve
            reject = rej
        })
        
        //values
        let toPush = [123,234,123,32,1,8]
        let shouldReceive = toPush.map(v => v + 1)
        let asyncGeneratorSubject = new AsyncGeneratorSubject<number>(() => {
            subscribed = false
            onUnscubscribe()
        })
        let cursor = asyncGeneratorSubject.createCursor((v: number) => v + 1)

        //consume
        let received: Array<number> = []
        const receive = () =>
            cursor.next<number>((error, row) => {
                if(error != null) {
                    reject(error)
                } else {
                    received.push(row)
                }
                receive()
            })
        receive()

        //assert
        unscubscribedPromise.should.be.fulfilled
            .then(() => {
                expect(subscribed).to.equal(false)
                expect(received).to.deep.equal(shouldReceive)
            })
            .should.notify(done)

        //generate
        let i = 0;
        const publish = () => {
            if(i < toPush.length) {
                asyncGeneratorSubject.publish(toPush[i])
                ++i;
                setTimeout(publish, 1)
            } else {
                asyncGeneratorSubject.complete()
            }
        }
        publish()
    })

    it("exceptional behaviour", (done) => {
        //state
        let subscribed: boolean = true
        let onUnscubscribe: () => void
        let unscubscribedPromise = new Promise(resolve => onUnscubscribe = resolve)

        //values
        let toPush = [123,234,123,32,1,8]
        let shouldReceive = [123,234]
        let asyncGeneratorSubject = new AsyncGeneratorSubject<number>(() => {
            subscribed = false
            onUnscubscribe()
        })
        let cursor = asyncGeneratorSubject.createCursor()

        //consume
        let received: Array<number> = []
        let reject: (error: Error) => void
        let receivePromise = new Promise((_, rej) => reject = rej)
        const receive = () =>
            cursor.next<number>((error, row) => {
                if(error != null) {
                    reject(error)
                    cursor.close()
                } else {
                    received.push(row)
                }
                receive()
            })
        receive()

        //assert
        receivePromise.should.eventually.be.rejected
            .then(() => expect(subscribed).to.equal(false))
            .then(() => unscubscribedPromise.should.be.fulfilled)
            .then(() => {
                expect(received).to.deep.equal(shouldReceive)
            })
            .should.notify(done)
        
        //generate
        let i = 0;
        const publish = () => {
            if(i < toPush.length) {
                if(i == 2) {
                    asyncGeneratorSubject.error(new Error())
                }
                asyncGeneratorSubject.publish(toPush[i])
                ++i;
                setTimeout(publish, 1)
            } else {
                asyncGeneratorSubject.complete()
            }
        }
        publish()
    })

    it("closed by consumer", (done) => {
        //state
        let subscribed: boolean = true
        let onUnscubscribe: () => void
        let unscubscribedPromise = new Promise(resolve => onUnscubscribe = resolve)

        //values
        let asyncGeneratorSubject = new AsyncGeneratorSubject<number>(() => {
            subscribed = false
            onUnscubscribe()
        })
        let cursor = asyncGeneratorSubject.createCursor()
        //need to listen to close it
        cursor.next(() => {})
        cursor.close()

        //assert
        expect(subscribed).to.equal(false)
        unscubscribedPromise.should.be.fulfilled
            .should.notify(done)
    })

})

describe("async generator subject consumed by generator", () => {

    it("normal behaviour", (done) => {
        //state
        let subscribed: boolean = true
        let onUnscubscribe: () => void
        let unscubscribedPromise = new Promise(resolve => onUnscubscribe = resolve)
        
        //values
        let toPush = [123,234,123,32,1,8]
        let shouldReceive = toPush.map(v => v + 1)
        let asyncGeneratorSubject = new AsyncGeneratorSubject<number>(() => {
            subscribed = false
            onUnscubscribe()
        })
        let iterator = asyncGeneratorSubject.createGenerator(v => v + 1)

        //consume
        let received: Array<number> = []
        const receive = async () => {
            for await (let i of iterator) {
                received.push(i)
            }
        }

        //assert
        receive().should.eventually.be.fulfilled
            .then(() => unscubscribedPromise.should.be.fulfilled)
            .then(() => {
                expect(subscribed).to.equal(false)
                expect(received).to.deep.equal(shouldReceive)
            })
            .should.notify(done)

        //generate
        let i = 0;
        const publish = () => {
            if(i < toPush.length) {
                asyncGeneratorSubject.publish(toPush[i])
                ++i;
                setTimeout(publish, 1)
            } else {
                asyncGeneratorSubject.complete()
            }
        }
        publish()
    })

    it("exceptional behaviour", (done) => {
        //state
        let subscribed: boolean = true
        let onUnscubscribe: () => void
        let unscubscribedPromise = new Promise(resolve => onUnscubscribe = resolve)

        //values
        let toPush = [123,234,123,32,1,8]
        let shouldReceive = [123,234]
        let asyncGeneratorSubject = new AsyncGeneratorSubject<number>(() => {
            subscribed = false
            onUnscubscribe()
        })
        let iterator = asyncGeneratorSubject.createGenerator()

        //consume
        let received: Array<number> = []
        const receive = async () => {
            for await (let i of iterator) {
                received.push(i)
            }
        }

        //assert
        receive().should.eventually.be.rejected
            .then(() => expect(subscribed).to.equal(false))
            .then(() => unscubscribedPromise.should.be.fulfilled)
            .then(() => {
                expect(received).to.deep.equal(shouldReceive)
            })
            .should.notify(done)
        
        //generate
        let i = 0;
        const publish = () => {
            if(i < toPush.length) {
                if(i == 2) {
                    asyncGeneratorSubject.error(new Error())
                }
                asyncGeneratorSubject.publish(toPush[i])
                ++i;
                setTimeout(publish, 1)
            } else {
                asyncGeneratorSubject.complete()
            }
        }
        publish()
    })

    it("on unsubsubscribe from consumer", (done) => {
        //state
        let subscribed: boolean = true
        let onUnscubscribe: () => void
        let unscubscribedPromise = new Promise(resolve => onUnscubscribe = resolve)

        //values
        let toPush = [4,234,56,3,1,8]
        let shouldReceive = [4,234,56]
        let asyncGeneratorSubject = new AsyncGeneratorSubject<number>(() => {
            subscribed = false
            onUnscubscribe()
        })
        let iterator = asyncGeneratorSubject.createGenerator()

        //consume
        let consumed = 0
        let received: Array<number> = []
        const receive = async () => {
            for await (let i of iterator) {
                received.push(i)
                ++consumed
                if(consumed == 3) {
                    return
                }
            }
        }

        //assert
        receive().should.eventually.be.fulfilled
            .then(() => expect(subscribed).to.equal(false))
            .then(() => unscubscribedPromise.should.be.fulfilled)
            .then(() => {
                expect(received).to.deep.equal(shouldReceive)
            })
            .should.notify(done)

        //generate
        let i = 0;
        const publish = () => {
            if(i < toPush.length) {
                asyncGeneratorSubject.publish(toPush[i])
                ++i;
                publish()
                setTimeout(publish, 1)
            } else {
                asyncGeneratorSubject.complete()
            }
        }
        publish()
    })

})