/* eslint-disable quotes */
import { snakeCase } from 'snake-case'
import { FilterExpression, BulkFilter, FilterSubExpressions, Operator } from '@vramework/generic/dist/filter'

export type QueryInterface<Tables> = string | ((args: {
  sf: <N extends keyof Tables, T extends Tables[N], F extends readonly (keyof T)[]>(table: N, fields: F, alias?: string) => string
  cf: <N extends keyof Tables, T extends Tables[N], F extends readonly (keyof T)[]>(table: N, fields: F, alias?: string) => string
}) => string)

export type ValueTypes = string | number | boolean | string[] | Date | null | undefined

export type Logger = {
  info: (message: string, payload?: Record<string, any>) => void,
  error: (message: string, payload?: Record<string, any>) => void
}

export const getFilters = (filters: Record<string, ValueTypes> | FilterSubExpressions) => {
  if (filters instanceof Array) {
    return createFilters({ filters })
  } else {
    return createFilters({ filters: Object.entries(filters).map(([field, value], index) => ({ value, field, operator: 'eq', conditionType: index !== 0 ? 'AND' : undefined }) )})
  }
}

// This is definately not production ready
export const createBulkInsert = (
  bulk: Record<string, ValueTypes>[],
): [string, string, Array<string | number | null | Date>] => {
  let i = 1
  const keys: string[] = []
  const values: string[] = []
  const realValues = bulk.map((data) => {
    data = transformValues(data)
    Object.keys(data).forEach((key) => {
      if (!keys.includes(key)) {
        keys.push(key)
      }
    })
    values.push(`(${keys.map(() => `$${i++}`).join(',')})`)
    return Object.keys(data).map((k) => data[k]) as Array<string | number | null>
  })
  return [`"${keys.map((k) => snakeCase(k)).join('","')}"`, values.join(','), realValues.reduce((r, v) => [...r, ...v])]
}

export const createInsert = (
  data: Record<string, number | string | null | string[] | undefined | boolean | Date>,
  offset = 0,
): [string, string, Array<string | number | null>] => {
  const keys = Object.keys(data).filter((k) => data[k] !== undefined)
  const values = keys.map((k, i) => '?')
  const realValues = keys.map((k) => data[k]) as Array<string | number | null>
  return [`"${keys.map((k) => snakeCase(k)).join('","')}"`, values.join(','), realValues]
}

// eslint-disable-next-line
export const transformValues = (from: any): Record<string, number | string | null> => {
  return Object.keys(from).reduce((r, k) => {
    const value = from[k]
    if (typeof value === 'number' || typeof value === 'string' || value === null || value === Date) {
      r[k] = value
    } else if (value instanceof Array && k === 'tags') {
      r[k] = `{ ${value.join(',')}}`
    } else {
      r[k] = JSON.stringify(value)
    }
    return r
  }, {} as Record<string, number | string | null>)
}

export const exactlyOneResult = <T>(result: T[], Err: Error): T => {
  if (result.length !== 1) {
    throw Err
  }
  return result[0]
}

const operatorToMysql = new Map<Partial<Operator>, string>([
  ['gt', '>'],
  ['gte', '>='],
  ['lt', '<'],
  ['lte', '<='],
  ['eq', '='],
  ['ne', '!='],
  ['on', '='],
  ['after', '>'],
  ['before', '<']
])

const manageFilters = (expressions: FilterExpression): Array<any> => {
  return expressions.reduce((result, expression) => {
    if (expression.conditionType) {
      result.push({ conditionType: expression.conditionType })
    }
    if (expression.expressions) {
      return [...result, { grouping: '(' }, ...manageFilters(expression.expressions), { grouping: ')' }]
    } else {
      const { field, value, operator } = expression
      const parts = field!.split('.')
      if (parts.length === 1) {
        result.push({ operator, field, value })
      } else {
        let table = parts[0].replace(/s$/, '')
        const actualField = parts.pop() as string
        result.push({ table, operator, field: actualField, value })
      }
    }
    return result
  }, [] as any[])
}

export const createFilters = (data: BulkFilter, freeTextFields: string[] = [], includeWhere: boolean = true, valueOffset: number = 0) => {
  const limit = data.limit || 1000
  const offset = data.offset || 0

  let sort: string = ''
  if (data.sort) {
    const parts = data.sort.key.split('.')
    let table = ''
    if (parts.length > 1) {
      // TODO: This logic should be in client.
      table = `"${parts[0]}".`
    }
    const field = parts.pop() as string
    sort = `ORDER BY ${table}${snakeCase(field)} ${data.sort.order}`
  }

  let cleanFilters = manageFilters(data.filters || [])
  if (data.freeText && data.freeText.trim()) {
    const freeTextFilters = freeTextFields.map<FilterSubExpressions>((field, index) => ({ conditionType: index === 0 ? undefined : 'OR', field, operator: 'contains', value: data.freeText! }))
    let filters: FilterExpression = []
    if (data.filters && data.filters.length > 0) {
      filters = data.filters
    }
    cleanFilters = manageFilters([...filters, { conditionType: data.filters?.length ? 'AND' : undefined, expressions: freeTextFilters }])
  } else {
    cleanFilters = manageFilters(data.filters || [])
  }

  const filterValues: any[] = []
  let filter: string = ''
  if (cleanFilters && cleanFilters.length > 0) {
    const filters = cleanFilters.map(({ grouping, conditionType = '', operator, table, field, value }) => {
      if (grouping) {
        return grouping
      }

      if (conditionType && field === undefined) {
        return conditionType
      }

      const t = table ? `"${table}".` : ''
      const column = `${t}"${snakeCase(field)}"`

      if (operator === 'includes' || operator === 'excludes') {
        filterValues.push(value)
        return `${conditionType} $${valueOffset + filterValues.length} ${operator === 'includes' ? '=' : '!='} ANY (${t}"${snakeCase(field)}")`
      }

      if (operatorToMysql.has(operator)) {
        filterValues.push(value)
        return `${conditionType} ${column} ${operatorToMysql.get(operator)} $${valueOffset + filterValues.length}`
      }

      if (conditionType) {
        return conditionType
      }

      return undefined
    }).filter(v => !!v)

    if (filters.length > 0) {
      filter = `${includeWhere ? 'WHERE ' : ''}${filters.join(' ')}`
    }
  }

  return { limit, offset, sort, filter, filterValues }
}
