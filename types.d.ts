declare module "JSONStream" {
    export function parse(pattern: string): NodeJS.ReadWriteStream;
    export function stringify(): NodeJS.ReadWriteStream;
}
