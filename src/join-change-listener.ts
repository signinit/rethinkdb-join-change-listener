import { AsyncGeneratorSubject } from "./async-generator-subject"

//TODO bundle changes
export class JoinChangeListener<
    LeftTableType extends { [key in string]: string } & { id: string },
    RightTableType extends { [key in string]: string }
> {

    private cursorMap: Map<string, Promise<ChangeCursor>> = new Map()
    private leftTableChangeCursor: ChangeCursor | undefined
    private subject = new AsyncGeneratorSubject<DatabaseJoinChange<RightTableType>>(this.complete.bind(this))
    private running: boolean = false

    constructor(
        private getLeftTable: () => Promise<Cursor>,
        private getLeftTableChange: () => Promise<ChangeCursor>,
        private leftKeyName: keyof LeftTableType,
        private getRightTableRows: (key: string) => Promise<Cursor>,
        private getRightTableRowsChange: (key: string) => Promise<ChangeCursor>,
        private rightKeyName: keyof RightTableType
    ) {
    }

    public createGenerator<K = DatabaseJoinChange<RightTableType>>(map?: (value: DatabaseJoinChange<RightTableType>) => K): AsyncGenerator<K> {
        return this.subject.createGenerator<K>(map)
    }

    public createCursor(map?: (value: DatabaseJoinChange<RightTableType>) => any): ChangeCursor {
        return this.subject.createCursor(map)
    }

    public async start(): Promise<void> {
        this.running = true
        let table = await (await this.getLeftTable()).toArray<LeftTableType>()
        table.forEach(row => this.createRowSubscription(row[this.leftKeyName], false, row.id))
        this.leftTableChangeCursor = await this.getLeftTableChange()
        this.recursiveNext(this.leftTableChangeCursor)
    }

    private recursiveNext(cursor: ChangeCursor) {
        cursor.next((error: Error | null, change: DatabaseChange<LeftTableType>) => {
            if(error == null) {
                if(this.running) {
                    this.executeNext(change)
                        .then(() => this.recursiveNext(cursor))
                        .catch(error => this.subject.error(error))
                }
            } else {
                this.subject.error(error)
            }
        })
    }

    private async executeNext(change: DatabaseChange<LeftTableType>): Promise<void> {
        if(change.new_val != null) {
            if(change.old_val != null) {
                if(change.new_val[this.leftKeyName] != change.old_val[this.leftKeyName]) {
                    //unsubscribe from old and resubscribe to new
                    await this.deleteRowSubscription(change.old_val[this.leftKeyName], change.old_val.id, change.new_val.id)
                    await this.createRowSubscription(change.new_val[this.leftKeyName], true, change.new_val.id, change.old_val.id)
                }
            } else {
                //new
                await this.createRowSubscription(change.new_val[this.leftKeyName], true, change.new_val.id)
            }
        } else if(change.old_val != null) {
            //delete
            await this.deleteRowSubscription(change.old_val[this.leftKeyName], change.old_val.id)
        } else {
            this.subject.error(new Error("old_val and new_val can't both be null"))
        }
    }

    public complete(): void {
        if(this.leftTableChangeCursor != null) {
            this.leftTableChangeCursor.close()
            this.leftTableChangeCursor = undefined
        }
        this.running = false
        this.cursorMap.forEach(cursorPromise => cursorPromise.then(c => c.close()).catch(error => this.subject.error(error)))
        this.cursorMap.clear()
    }

    private async createRowSubscription(key: string, includeInitial: boolean, newLeftId: string, oldLeftId?: string): Promise<void> {
        if(includeInitial) {
            let rowEntries = await (await this.getRightTableRows(key)).toArray<RightTableType>()
            rowEntries.forEach(entry => this.subject.publish({ old_val: { leftId: oldLeftId  }, new_val: { leftId: newLeftId, right: entry } }))
        }
        let cursorPromise = this.cursorMap.get(key)
        if(cursorPromise == null) {
            cursorPromise = this.getRightTableRowsChange(key)
            this.cursorMap.set(key, cursorPromise)
        }
        let cursor = await cursorPromise
        const next = () =>
            cursor.next((error: Error, change: DatabaseChange<RightTableType>) => {
                if(error != null) {
                    this.subject.error(error)
                } else {
                    this.subject.publish({ old_val: { leftId: newLeftId, right: change.old_val }, new_val: { leftId: newLeftId, right: change.new_val } })
                }
                next()
            })
        next()
    }

    private async deleteRowSubscription(key: string, oldLeftId: string, newLeftId?: string): Promise<void> {
        let cursorPromise = this.cursorMap.get(key)
        if(cursorPromise != null) {
            this.cursorMap.delete(key)
            let values = await (await this.getRightTableRows(key)).toArray<RightTableType>()
            await (await cursorPromise).close()
            values.forEach(value => this.subject.publish({ old_val: { leftId: oldLeftId, right: value }, new_val: { leftId: newLeftId } }))
        } else {
            this.subject.error(new Error(`unkown row in destination table with ${this.rightKeyName}: "${key}"`))
        }
    }

}

export type DatabaseChange<T> = {
    old_val?: T
    new_val?: T
}

export type DatabaseJoinChange<T> = {
    old_val: { leftId?: string, right?: T }
    new_val: { leftId?: string, right?: T }
}

export type Cursor = {
    each<T>(cb: (err: Error, row: T) => void): void
    toArray<T>(): Promise<T[]>
} & ChangeCursor

export type ChangeCursor = {
    next<T>(cb: (err: Error, row: T) => void): void
    close(): Promise<void>
}