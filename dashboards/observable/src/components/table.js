import {DuckDBClient} from "npm:@observablehq/duckdb";

const db = DuckDBClient.of({gaia: FileAttachment("datalake/staging/master/master.parquet")});

//${Inputs.table(db, {rows: 16})}