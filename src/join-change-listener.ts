import { AsyncGeneratorSubject } from "./async-generator-subject"

//TODO bundle changes
export class JoinChangeListener<
    LeftTableType extends { [key in string]: string } & { id: string },
    RightTableType extends { [key in string]: string }
> {

    private rightMap: Map<string, {
        left: LeftTableType, cursorPromise: Promise<ChangeCursor>
    }> = new Map()
    private leftTableChangeCursor: ChangeCursor | undefined
    private subject = new AsyncGeneratorSubject<DatabaseJoinChange<LeftTableType, RightTableType>>(this.complete.bind(this))
    private running: boolean = false

    constructor(
        private getLeftTable: () => Promise<Cursor>,
        private getLeftTableChange: () => Promise<ChangeCursor>,
        private leftKeyName: keyof LeftTableType,
        private getRightTableRows: (key: string) => Promise<Cursor>,
        private getRightTableRowsChange: (key: string) => Promise<ChangeCursor>,
        private rightKeyName: keyof RightTableType,
        private mapLeftTable?: (input: any) => LeftTableType,
        private mapRightTable?: (input: any) => RightTableType
    ) {
    }

    public createGenerator<K = DatabaseJoinChange<LeftTableType, RightTableType>>(map?: (value: DatabaseJoinChange<LeftTableType, RightTableType>) => K): AsyncGenerator<K> {
        return this.subject.createGenerator<K>(map)
    }

    public createCursor(map?: (value: DatabaseJoinChange<LeftTableType, RightTableType>) => any): ChangeCursor {
        return this.subject.createCursor(map)
    }

    public async start(): Promise<void> {
        this.running = true
        let table = await (await this.getLeftTable()).toArray<LeftTableType>()
        let mappedTable: Array<LeftTableType>
        if(this.mapLeftTable != null) {
            mappedTable = table.map(this.mapLeftTable)
        } else {
            mappedTable = table
        }
        mappedTable.forEach(row => this.createRowSubscription(row[this.leftKeyName], false, row))
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
                    await this.deleteRowSubscription(change.old_val[this.leftKeyName], change.old_val)
                    await this.createRowSubscription(change.new_val[this.leftKeyName], true, change.new_val)
                } else {
                    //TODO if also include left changes is wanted
                    let right = this.rightMap.get(change.new_val[this.leftKeyName])
                    if(right != null) {
                        right.left = change.new_val
                    }
                }
            } else {
                //new
                await this.createRowSubscription(change.new_val[this.leftKeyName], true, change.new_val)
            }
        } else if(change.old_val != null) {
            //delete
            await this.deleteRowSubscription(change.old_val[this.leftKeyName], change.old_val)
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
        this.rightMap.forEach(({ cursorPromise: cursor }) => cursor.then(cursor => cursor.close()).catch(error => this.subject.error(error)))
        this.rightMap.clear()
    }

    private async createRowSubscription(key: string, includeInitial: boolean, left: LeftTableType): Promise<void> {
        if(includeInitial) {
            let rowEntries = await (await this.getRightTableRows(key)).toArray<RightTableType>()
            let mappedRow: Array<RightTableType>
            if(this.mapRightTable != null) {
                mappedRow = rowEntries.map(this.mapRightTable)
            } else {
                mappedRow = rowEntries
            }
            mappedRow.forEach(entry => this.subject.publish({ new_val: { left, right: entry }, old_val: undefined }))
        }
        let rightEntry = this.rightMap.get(key)!
        if(rightEntry == null) {
            let cursorPromise = this.getRightTableRowsChange(key)
            rightEntry = {
                cursorPromise: cursorPromise,
                left
            }
            this.rightMap.set(key, rightEntry)
        }
        let cursor = await rightEntry.cursorPromise
        const next = () =>
            cursor.next((error: Error, change: DatabaseChange<RightTableType>) => {
                if(error != null) {
                    this.subject.error(error)
                } else {
                    this.subject.publish({
                        old_val: change.old_val != null ?
                            { left: rightEntry.left, right: change.old_val } : undefined,
                        new_val: change.new_val != null ?
                            { left: rightEntry.left, right: change.new_val } : undefined
                    })
                }
                next()
            })
        next()
    }

    private async deleteRowSubscription(key: string, oldLeft: LeftTableType): Promise<void> {
        let right = this.rightMap.get(key)
        if(right != null) {
            this.rightMap.delete(key)
            let values = await (await this.getRightTableRows(key)).toArray<RightTableType>()
            await (await right.cursorPromise).close()
            //TODO send full left
            values.forEach(value => this.subject.publish({ old_val: { left: oldLeft, right: value }, new_val: undefined }))
        } else {
            this.subject.error(new Error(`unkown row in destination table with ${this.rightKeyName}: "${key}"`))
        }
    }

}

export type DatabaseChange<T> = {
    old_val?: T
    new_val?: T
}

export type DatabaseJoinChange<Left, Right> = {
    old_val: { left: Left, right: Right } | undefined
    new_val: { left: Left, right: Right } | undefined
}

export type Cursor = {
    each<T>(cb: (err: Error, row: T) => void): void
    toArray<T>(): Promise<T[]>
} & ChangeCursor

export type ChangeCursor = {
    next<T>(cb: (err: Error, row: T) => void): void
    close(): Promise<void>
}