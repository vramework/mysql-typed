import { FilterSubExpressions } from '@vramework/generic/dist/filter'
import { createPool, PoolOptions, Pool, PoolConnection, RowDataPacket } from 'mysql2/promise'
import { exactlyOneResult, getFilters, Logger, QueryInterface, ValueTypes } from './database-utils'
import { snakeCase } from 'snake-case'

export class TypedMySQLPool<Tables extends { [key: string]: any }, CustomTypes = never> {
  public pool: Pool
  public client!: PoolConnection

  constructor(private poolOptions: PoolOptions, private logger: Logger) {
    this.logger.info(`Using db host: ${poolOptions.host}`)
    this.pool = createPool(poolOptions)
  }

  public async init() {
    this.client = await this.pool.getConnection()
    await this.checkConnection()
    await this.client.release()
  }

  public async getClient() {
    return this.pool.getConnection()
  }

  public async crudGetAll<N extends keyof Tables, T extends Tables[N]>(table: N, filters: Partial<T> | FilterSubExpressions): Promise<T[]>
  public async crudGetAll<N extends keyof Tables, T extends Tables[N]>(table: N, filters: Partial<T> | FilterSubExpressions, notSingleError: Error): Promise<T>
  public async crudGetAll<N extends keyof Tables, T extends Tables[N]>(table: N, filters: Partial<T> | FilterSubExpressions, notSingleError?: undefined | Error): Promise<T | T[]> {
    const { filter, filterValues } = getFilters(filters)
    const result  = await this.query<T>(`SELECT * FROM "${table}" ${filter}`, filterValues)
    if (notSingleError) {
      return exactlyOneResult(result.rows, notSingleError)
    }
    return result.rows
  }

  public async crudGet<N extends keyof Tables, T extends Tables[N], F extends readonly (keyof T)[]>(table: N, fields: F, filters: Partial<T> | FilterSubExpressions): Promise<Pick<T, typeof fields[number]>[]>
  public async crudGet<N extends keyof Tables, T extends Tables[N], F extends readonly (keyof T)[]>(table: N, fields: F, filters: Partial<T> | FilterSubExpressions, notSingleError: Error): Promise<Pick<T, typeof fields[number]>>
  public async crudGet<N extends keyof Tables, T extends Tables[N], F extends readonly (keyof T)[]>(table: N, fields: F, filters: Partial<T> | FilterSubExpressions, notSingleError?: undefined | Error): Promise<Pick<T, typeof fields[number]> | Pick<T, typeof fields[number]>[]> {
    const { filter, filterValues } = getFilters(filters)
    const result = await this.query<Pick<T, typeof fields[number]>>(({ sf }) => `
      SELECT ${sf(table, fields)}
      FROM "${table}"
      ${filter}
    `, filterValues)
    if (notSingleError) {
      return exactlyOneResult(result.rows, notSingleError)
    }
    return result.rows
  }

  public async one<T>(
    statement: QueryInterface<Tables>,
    values: Array<ValueTypes> = [],
    error: Error
  ): Promise<T> {
    const result = await this.query<T>(statement, values)
    return exactlyOneResult(result.rows, error)
  }

  public async many<T>(
    statement: QueryInterface<Tables>,
    values: Array<ValueTypes> = []
  ): Promise<T[]> {
    const result = await this.query<T>(statement, values)
    return result.rows
  }

  public async query<T>(statement: QueryInterface<Tables>, values?: any[]): Promise<{ rows: T[], meta: any }> {
    const query = typeof statement === 'string' ? statement : statement({
      cf: this.createFields,
      sf: this.selectFields
    })
    const [rows, meta] = await this.pool.query<RowDataPacket[]>(query, values)
    return { rows, meta } as { rows: T[], meta: any }
  }

  public async close() {
    this.pool.end()
  }

  private async checkConnection(): Promise<void> {
    try {
      const result = await this.query<{ serverVersion: string }>('SHOW server_version;')
      this.logger.info(`Postgres server version is: ${result.rows[0].serverVersion}`)
    } catch (e) {
      console.error(e)
      this.logger.error(`Unable to connect to server with ${this.poolOptions.host}, exiting server`)
      process.exit(1)
    }
  }

  public createFields<N extends keyof Tables, T extends Tables[N], F extends readonly (keyof T)[]>(table: N, fields: F, alias?: string) {
    const r = fields.reduce((r, field) => {
      r.push(`'${field}'`)
      r.push(`"${alias}".${snakeCase(field as string)}`)
      return r
    }, [] as string[])
    return r.join(',')
  }

  private selectFields<N extends keyof Tables, T extends Tables[N], F extends readonly (keyof T)[]>(table: N, fields: F, alias?: string) {
    const r = fields.reduce((r, field) => {
      r.push(`"${alias}".${snakeCase(field as string)}`)
      return r
    }, [] as string[])
    return r.join(',')
  }

}