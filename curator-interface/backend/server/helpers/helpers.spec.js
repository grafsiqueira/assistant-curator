const {
  getConversations,
  arrangeConversations,
  updateConversation,
} = require("./helpers");

describe("tests helper functions", () => {
  it("arrange rows per conversations", () => {
    const input = [
      { CONVERSATIONID: "1", CLIENTMESSAGE: "OI" },
      { CONVERSATIONID: "1", CLIENTMESSAGE: "tudo bem?" },
      { CONVERSATIONID: "2", CLIENTMESSAGE: "e ai" },
    ];
    const output = {
      1: [
        { CONVERSATIONID: "1", CLIENTMESSAGE: "OI" },
        { CONVERSATIONID: "1", CLIENTMESSAGE: "tudo bem?" },
      ],
      2: [{ CONVERSATIONID: "2", CLIENTMESSAGE: "e ai" }],
    };
    expect(arrangeConversations(input)).toEqual(output);
  });

  it("gets all conversations without score", async () => {
    const result = await getConversations();
    expect(result).toBeInstanceOf(Object);
  });

  it("update logs in a conversation", () => {
    const input = [
      {
        IDUSER: "anonymous_IBMuid-bcb6ceee-540a-4c00-bb2d-4c4d9a267dc4",
        CONVERSATIONID: "90c444e7-fef2-4c4e-8d90-8872d97c077b",
        LOGID: "84869e5d-54ac-4de8-8aa7-b22702bee141",
        CLIENTMESSAGE: "w",
        CLIENTTIMESTAMP: "2021-11-23 18:16:05.000000",
        ASSISTANTMESSAGE:
          "Por favor, digite o CPF do titular da conta com 11 dígitos. E fique tranquilo, seus dados estão seguros comigo!",
        ASSISTANTTIMESTAMP: "2021-11-23 18:16:05.000000",
        CHANNEL: "Chat",
        NODETITLE: "node_10_1604597824647",
        SENTIMENT: 0.7,
        FIRSTINTENT: "xingamento",
        FIRSTINTENTCONFIDENCE: 0,
        INTENTS: "xingamento",
        INTENTSCONFIDENCE: "0.4609287738800049",
        ENTITIES: "",
        ERROR: "",
        SCORE: 1,
      },
    ];
    expect(updateConversation(input)).resolves.toBe("Conversation updated");
  });
});
