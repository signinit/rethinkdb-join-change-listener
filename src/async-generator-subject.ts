import { ChangeCursor } from "./join-change-listener"

type CallbackObject<T> = {
    resolve: (value: T | null) => void
    reject: (error: Error) => void
}

export class AsyncGeneratorSubject<T> {

    private queue: Array<{ error?: undefined, value: T | null } | { error: Error }> = []
    private callback: CallbackObject<T> | undefined
    private first: boolean = true

    /**
     * 
     * @param onUnsubscribe callback executed before the iteratable is completed or rejected
     */
    constructor(
        private onUnsubscribe: () => void
    ) {}

    publish(value: T): void {
        if(this.callback == undefined) {
            this.queue.push({ value })
        } else {
            let resolve = this.callback.resolve
            this.callback = undefined
            resolve(value)
        }
    }

    error(error: Error): void {
        if(this.callback == undefined) {
            this.queue.push({ error })
        } else {
            let reject = this.callback.reject
            this.callback = undefined
            reject(error)
        }
    }

    complete() {
        if(this.callback == undefined) {
            this.queue.push({ value: null })
        } else {
            let resolve = this.callback.resolve
            this.callback = undefined
            resolve(null)
        }
    }

    public createCursor(map?: (value: T) => any): ChangeCursor {
        let cursor: ChangeCursor = {
            close: () => {
                this.complete()
                return Promise.resolve()
            },
            next: <T>(cb: (error: Error, row: T) => void) => {
                let entry = this.queue.shift()
                if(entry != null) {
                    if(entry.error == null) {
                        if(entry.value != null) {
                            cb(null!, map != null ? map(entry.value) : entry.value as any)
                        } else {
                            this.onUnsubscribe()
                        }
                    } else {
                        this.onUnsubscribe()
                        cb(entry.error, null!)
                    }
                } else {
                    this.callback = {
                        resolve: row => {
                            if(row != null) {
                                cb(null!, map != null ? map(row) : row as any)
                            } else {
                                this.onUnsubscribe()
                            }
                        },
                        reject: error => {
                            this.onUnsubscribe()
                            cb(error, null!)
                        }
                    }
                }
            }
        }
        return cursor
    }

    public createGenerator<K = T>(map?: (value: T) => K): AsyncGenerator<K> {
        let generator = this.pCreateGenerator(map)
        withCancel(generator, this.onUnsubscribe)
        return generator
    }

    private async * pCreateGenerator<K>(map?: (value: T) => K): AsyncGenerator<K> {
        while(true) {
            let entry = this.queue.shift()
            if(entry != null) {
                if(entry.error == null) {
                    if(entry.value == null) {
                        this.onUnsubscribe()
                        return
                    }
                    yield map != null ? map(entry.value) : entry.value as any
                } else {
                    if(this.first) {
                        this.onUnsubscribe()
                    }
                    return Promise.reject(entry.error)
                }
            } else {
                let value = await new Promise<T | null>((resolve, reject) => this.callback = { resolve, reject: error => {
                    if(this.first) {
                        this.onUnsubscribe()
                    }
                    reject(error)
                } })
                if(value == null) {
                    this.onUnsubscribe()
                    return
                }
                yield map != null ? map(value) : value as any
            }
            if(this.first) {
                this.first = false
            }
        }
    }

}

function withCancel<T>(asyncIterator: AsyncIterator<T>, onCancel: () => void): void {
    const asyncReturn = asyncIterator.return

    asyncIterator.return = () => {
        onCancel()
        return asyncReturn ? asyncReturn.call(asyncIterator) : Promise.resolve({ value: undefined, done: true })
    }
}