import { Cursor, JoinChangeListener, DatabaseJoinChange, ChangeCursor } from "./join-change-listener";
import chaiAsPromised from "chai-as-promised"
import chai, { expect } from "chai"

chai.use(chaiAsPromised)
chai.should()

const DONE_ERROR = new Error("done")
const DONE: FakeData = {
    error: DONE_ERROR,
    message: "done"
}

describe("join listener", () => {

    it("insert left", done => {
        let listenerClosedSink: Array<Promise<void>> = []
        let joinListener = new JoinChangeListener<{ id: string },{ id: string, userId: string, name: string }>(
            asCursorData([], listenerClosedSink, "left table"),
            asCursorChange([
                createInsert({ id: "6" }),
            ], listenerClosedSink, "left table changes"),
            "id",
            cursorMapData(new Map([
                ["6", [{ id: "2", userId: "6", name: "nr2" }]]
            ]), listenerClosedSink, "right table row"),
            cursorMapChange(new Map([
                ["6", [DONE]]
            ]), listenerClosedSink, "right table row changes"),
            "userId"
        )
        joinListener.start()
        let generator = joinListener.createGenerator()
        let shouldReceive: Array<DatabaseJoinChange<{ id: string, name: string, userId: string }>> = [
            {
                old_val: {
                    leftId: undefined
                },
                new_val: {
                    leftId: "6",
                    right: {
                        id: "2",
                        name: "nr2",
                        userId: "6"
                    }
                }
            }
        ]
        let received: Array<DatabaseJoinChange<{ id: string, userId: string, name: string }>> = []
        const receive = async () => {
            for await (let change of generator) {
                received.push(change)
            }
        }
        receive().should.eventually.rejectedWith(DONE_ERROR)
            .then(() => Promise.all(listenerClosedSink).should.eventually.fulfilled)
            .then(() => expect(received).to.deep.equal(shouldReceive))
            .should.notify(done)
    })

    it("insert right", done => {
        let listenerClosedSink: Array<Promise<void>> = []
        let joinListener = new JoinChangeListener<{ id: string },{ id: string, userId: string, name: string }>(
            asCursorData([
                { id: "6" }
            ], listenerClosedSink, "left table"),
            asCursorChange([], listenerClosedSink, "left table changes"),
            "id",
            cursorMapData(new Map([]), listenerClosedSink, "right table row"),
            cursorMapChange(new Map([
                ["6", [
                    createInsert({ id: "2", userId: "6", name: "nr2" }),
                    DONE
                ]]
            ]), listenerClosedSink, "right table row changes"),
            "userId"
        )
        joinListener.start()
        let generator = joinListener.createGenerator()
        let shouldReceive: Array<DatabaseJoinChange<{ id: string, name: string, userId: string }>> = [
            {
                old_val: {
                    leftId: "6",
                    right: undefined
                },
                new_val: {
                    leftId: "6",
                    right: {
                        id: "2",
                        name: "nr2",
                        userId: "6"
                    }
                }
            }
        ]
        let received: Array<DatabaseJoinChange<{ id: string, userId: string, name: string }>> = []
        const receive = async () => {
            for await (let change of generator) {
                received.push(change)
            }
        }
        receive().should.eventually.rejectedWith(DONE_ERROR)
            .then(() => Promise.all(listenerClosedSink).should.eventually.fulfilled)
            .then(() => expect(received).to.deep.equal(shouldReceive))
            .should.notify(done)
    })

    it("edit left", done => {
        let listenerClosedSink: Array<Promise<void>> = []
        let joinListener = new JoinChangeListener<{ id: string, userId: string },{ id: string, name: string }>(
            asCursorData([{ id: "6", userId: "2" }], listenerClosedSink, "left table"),
            asCursorChange([
                createEdit({ id: "6", userId: "2" }, { id: "6", userId: "3" }),
                DONE
            ], listenerClosedSink, "left table changes"),
            "userId",
            cursorMapData(new Map([
                ["2", [{ id: "2", name: "nr2" }]],
                ["3", [{ id: "3", name: "nr3" }]]
            ]), listenerClosedSink, "right table row"),
            cursorMapChange(new Map([]), listenerClosedSink, "right table row changes"),
            "id"
        )
        joinListener.start()
        let generator = joinListener.createGenerator()
        let shouldReceive: Array<DatabaseJoinChange<{ id: string, name: string }>> = [
            {
                old_val: {
                    leftId: "6",
                    right: {
                        id: "2",
                        name: "nr2",
                    }
                },
                new_val: {
                    leftId: "6"
                }
            },
            {
                old_val: {
                    leftId: "6"
                },
                new_val: {
                    leftId: "6",
                    right: {
                        id: "3",
                        name: "nr3"
                    }
                }
            }
        ]
        let received: Array<DatabaseJoinChange<{ id: string, name: string }>> = []
        const receive = async () => {
            for await (let change of generator) {
                received.push(change)
            }
        }
        receive().should.eventually.rejectedWith(DONE_ERROR)
            .then(() => Promise.all(listenerClosedSink).should.eventually.fulfilled)
            .then(() => expect(received).to.deep.equal(shouldReceive))
            .should.notify(done)
    })

    it("edit right", done => {
        let listenerClosedSink: Array<Promise<void>> = []
        let joinListener = new JoinChangeListener<{ id: string },{ userId: string, name: string }>(
            asCursorData([{ id: "6" }], listenerClosedSink, "left table"),
            asCursorChange([], listenerClosedSink, "left table changes"),
            "id",
            cursorMapData(new Map([
                ["6", [{ userId: "6", name: "nr2" }]]
            ]), listenerClosedSink, "right table row"),
            cursorMapChange(new Map([
                ["6", [
                    createEdit({ userId: "6", name: "nr2" }, { userId: "6", name: "nr3" }),
                    DONE
                ]]
            ]), listenerClosedSink, "right table row changes"),
            "userId"
        )
        joinListener.start()
        let generator = joinListener.createGenerator()
        let shouldReceive: Array<DatabaseJoinChange<{ name: string, userId: string }>> = [
            {
                old_val: {
                    leftId: "6",
                    right: {
                        name: "nr2",
                        userId: "6"
                    }
                },
                new_val: {
                    leftId: "6",
                    right: {
                        name: "nr3",
                        userId: "6"
                    }
                }
            }
        ]
        let received: Array<DatabaseJoinChange<{ userId: string, name: string }>> = []
        const receive = async () => {
            for await (let change of generator) {
                received.push(change)
            }
        }
        receive().should.eventually.rejectedWith(DONE_ERROR)
            .then(() => Promise.all(listenerClosedSink).should.eventually.fulfilled)
            .then(() => expect(received).to.deep.equal(shouldReceive))
            .should.notify(done)
    })

    it("delete left", done => {
        let listenerClosedSink: Array<Promise<void>> = []
        let joinListener = new JoinChangeListener<{ id: string },{ id: string, userId: string, name: string }>(
            asCursorData([{ id: "6" }], listenerClosedSink, "left table"),
            asCursorChange([
                createDelete({ id: "6" }),
                DONE
            ], listenerClosedSink, "left table changes"),
            "id",
            cursorMapData(new Map([
                ["6", [{ id: "2", userId: "6", name: "nr2" }]]
            ]), listenerClosedSink, "right table row"),
            cursorMapChange(new Map([]), listenerClosedSink, "right table row changes"),
            "userId"
        )
        joinListener.start()
        let generator = joinListener.createGenerator()
        let shouldReceive: Array<DatabaseJoinChange<{ id: string, name: string, userId: string }>> = [
            {
                old_val: {
                    leftId: "6",
                    right: {
                        id: "2",
                        name: "nr2",
                        userId: "6"
                    }
                },
                new_val: {
                    leftId: undefined
                }
            }
        ]
        let received: Array<DatabaseJoinChange<{ id: string, userId: string, name: string }>> = []
        const receive = async () => {
            for await (let change of generator) {
                received.push(change)
            }
        }
        receive().should.eventually.rejectedWith(DONE_ERROR)
            .then(() => Promise.all(listenerClosedSink).should.eventually.fulfilled)
            .then(() => expect(received).to.deep.equal(shouldReceive))
            .should.notify(done)
    })

    it("delete right", done => {
        let listenerClosedSink: Array<Promise<void>> = []
        let joinListener = new JoinChangeListener<{ id: string },{ id: string, userId: string, name: string }>(
            asCursorData([{ id: "6" }], listenerClosedSink, "left table"),
            asCursorChange([], listenerClosedSink, "left table changes"),
            "id",
            cursorMapData(new Map([
                ["6", [{ id: "2", userId: "6", name: "nr2" }]]
            ]), listenerClosedSink, "right table row"),
            cursorMapChange(new Map([
                ["6", [
                    createDelete({ id: "2", userId: "6", name: "nr2" }),
                    DONE
                ]]
            ]), listenerClosedSink, "right table row changes"),
            "userId"
        )
        joinListener.start()
        let generator = joinListener.createGenerator()
        let shouldReceive: Array<DatabaseJoinChange<{ id: string, name: string, userId: string }>> = [
            {
                old_val: {
                    leftId: "6",
                    right: {
                        id: "2",
                        name: "nr2",
                        userId: "6"
                    }
                },
                new_val: {
                    leftId: "6",
                    right: undefined
                }
            }
        ]
        let received: Array<DatabaseJoinChange<{ id: string, userId: string, name: string }>> = []
        const receive = async () => {
            for await (let change of generator) {
                received.push(change)
            }
        }
        receive().should.eventually.rejectedWith(DONE_ERROR)
            .then(() => Promise.all(listenerClosedSink).should.eventually.fulfilled)
            .then(() => expect(received).to.deep.equal(shouldReceive))
            .should.notify(done)
    })
    
    it("nested joins", done => {
        let listenerClosedSink: Array<Promise<void>> = []
        let firstJoin = new JoinChangeListener(
            asCursorData([
                { id: "2" }
            ], listenerClosedSink, "user table"),
            asCursorChange([
                createInsert({ id: "1" }),
                createDelete({ id: "2" })
            ], listenerClosedSink, "user table changes"),
            "id",
            cursorMapData(new Map([
                ["1", [
                    { id: "z", userId: "1", roleId: "2" },
                    { id: "k", userId: "1", roleId: "3" },
                ]],
                ["2", [
                    { id: "y", userId: "2", roleId: "3" },
                    { id: "u", userId: "2", roleId: "4" }
                ]]
            ]), listenerClosedSink, "userRoles table row"),
            cursorMapChange(new Map([
                ["1", [
                    createInsert({ id: "o", userId: "1", roleId: "4" })
                ]],
                ["2", [
                    createInsert({ id: "x", userId: "2", roleId: "5" }),
                    createDelete({ id: "z", userId: "1", roleId: "2" })
                ]],
            ]), listenerClosedSink, "userRoles table row changes"),
            "userId"
        )
        firstJoin.start()
        let cursor = firstJoin.createCursor(val => ({ old_val: val.old_val.right, new_val: val.new_val.right }))
        let secondJoin = new JoinChangeListener<{ id: string, roleId: string, userId: string }, { id: string, name: string }>(
            asCursorData([
                { id: "z", userId: "1", roleId: "2" },
                { id: "k", userId: "1", roleId: "3" },
                { id: "y", userId: "2", roleId: "3" },
                { id: "u", userId: "2", roleId: "4" }
            ], listenerClosedSink, "joined userRoleJoin table"),
            () => Promise.resolve(cursor),
            "roleId",
            cursorMapData(new Map([
                ["2", [{ id: "2", name: "role2" }]],
                ["5", [{ id: "5", name: "role5" }]],
                ["4", [{ id: "4", name: "role4" }]],
            ]), listenerClosedSink, "role table row"),
            cursorMapChange(new Map([
                ["3", [
                    createInsert({ id: "3", name: "role3" }),
                    createDelete({ id: "3", name: "role3" }),
                    DONE
                ]]
            ]), listenerClosedSink, "role table row changes"),
            "id"
        )
        secondJoin.start()
        let generator = secondJoin.createGenerator()
        let received: Array<DatabaseJoinChange<{ id: string, name: string }>> = []
        const receive = async () => {
            for await (let change of generator) {
                console.log(change)
                received.push(change)
            }
        }
        receive()
            .should.eventually.rejectedWith(DONE_ERROR)
            .then(() => Promise.all(listenerClosedSink).should.eventually.fulfilled)
            .should.notify(done)
    })

})

function createInsert(value: any): FakeData {
    return {
        data: {
            new_val: value
        },
        message: `insert ${JSON.stringify(value)}`
    }
}

function createDelete(value: any): FakeData {
    return {
        data: {
            old_val: value
        },
        message: `delete ${JSON.stringify(value)}`
    }
}

function createEdit(oldVal: any, newVal: any): FakeData {
    return {
        data: {
            old_val: oldVal,
            new_val: newVal
        },
        message: `edit ${JSON.stringify(oldVal)} -> ${JSON.stringify(newVal)}`
    }
}

type FakeData = {
    error?: Error,
    data?: any,
    message?: string
}

function asCursorChange(array: Array<FakeData>, promiseSink: Array<Promise<void>>, name: string): () => Promise<ChangeCursor> {
    return () => {
        let resolve: () => void
        promiseSink.push(new Promise(res => resolve = res))
        return Promise.resolve(new FakeCursor(array, 1, resolve!, name))
    }
}

function asCursorData(array: Array<any>, promiseSink: Array<Promise<void>>, name: string): () => Promise<Cursor> {
    return () => {
        let resolve: () => void
        promiseSink.push(new Promise(res => resolve = res))
        return Promise.resolve(new FakeCursor(array.map(data => ({ data })), 1, resolve!, name))
    }
}

function cursorMapData(map: Map<string, Array<any>>, promiseSink: Array<Promise<void>>, name: string): (key: string) => Promise<Cursor> {
    return key => {
        let resolve: () => void
        promiseSink.push(new Promise(res => resolve = res))
        let array: Array<any>
        if(map.has(key)) {
            array = map.get(key)!
        } else {
            array = []
        }
        return Promise.resolve(new FakeCursor(array.map(data => ({ data })), 1, resolve!, `${name} (key: ${key})`))
    }
}

function cursorMapChange(map: Map<string, Array<FakeData>>, promiseSink: Array<Promise<void>>, name: string): (key: string) => Promise<Cursor> {
    return key => {
        let resolve: () => void
        promiseSink.push(new Promise(res => resolve = res))
        let array: Array<any>
        if(map.has(key)) {
            array = map.get(key)!
        } else {
            array = []
        }
        return Promise.resolve(new FakeCursor(array, 1, resolve!, `${name} (key: ${key})`))
    }
} 

class FakeCursor implements Cursor {

    private index: number = 0

    constructor(
        private data: Array<FakeData>,
        private delay: number,
        private onClose: () => void,
        private name: string
    ) {
        console.log("listening to " + name)
    }

    each<T>(cb: (err: Error, row: T) => void): void {
        const next = () => {
            if(this.index < this.data.length) {
                let entry = this.data[this.index]
                console.log(entry.message)
                cb(entry.error!, entry.data)
                ++this.index
                if(this.index < this.data.length) {
                    setTimeout(next, this.delay)
                }
            }
        }
        next()
    }
    
    next<T>(cb: (err: Error, row: T) => void): void {
        setTimeout(() => {
            if(this.index < this.data.length) {
                console.log(this.data[this.index].message)
                cb(this.data[this.index].error!, this.data[this.index].data)
                ++this.index
            }
        }, this.delay)
    }

    toArray<T>(): Promise<T[]> {
        this.close()
        return Promise.all(this.data.map(entry => entry.data!))
    }

    close(): Promise<void> {
        console.log("closed " + this.name)
        this.onClose!()
        return Promise.resolve()
    }


}