const { Database } = require("ibm_db");
const { connect, endConnection, update, select } = require("./db2");
const { connStr, table } = require("../config/db2-credentials.json");

describe("tests creation and ending of connection", () => {
  it("creates a  connection", () => {
    expect(connect(connStr)).resolves.toBeInstanceOf(Database);
  });

  it("ends a connection", async () => {
    const conn = await connect(connStr);
    expect(endConnection(conn)).resolves.toBe("Db2 connection closed.");
  });
});

describe("tests interactions with db2", () => {
  let conn = null;

  beforeAll(async () => {
    conn = await connect(connStr);
  });
  afterAll(async () => {
    await endConnection(conn);
  });

  it("selects values from table with where clause", () => {
    expect(select(conn, table, "1=1")).resolves.toBeInstanceOf(Array);
  });

  it("updates values of table", () => {
    expect(
      update(
        conn,
        table,
        "clientMessage = 'atualizado'",
        "logId = '84869e5d-54ac-4de8-8aa7-b22702bee141'"
      )
    ).resolves.toBe("Updated rows");
  });
});
