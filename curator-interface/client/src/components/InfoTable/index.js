import "./style.css";
import { useGlobalState } from "../../hooks/globalState";
import {
  DataTable,
  TableContainer,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  TableHeader,
  TableToolbar,
  TableToolbarContent,
  Button,
} from "carbon-components-react";
import { sendScore } from "../../helpers/helpers";
import textLanguage from "../../helpers/languagesConfig";

export default function InfoTable({ ID, day, rowData }) {
  const { conversations, setConversations, credentialsAndDefaults, language } =
    useGlobalState();

  const headerData = [
    {
      key: "LOGID",
      header: "logID",
    },
    {
      key: "CLIENTMESSAGE",
      header: "Client Message",
    },
    {
      key: "ASSISTANTMESSAGE",
      header: "Assistant Message",
    },
    {
      key: "FIRSTINTENT",
      header: "Intent identified",
    },
    {
      key: "SCORE",
      header: "Score (1 - 5)",
    },
  ];

  const filtered = rowData.filter((value) => {
    if (day)
      return (
        (value.CONVERSATIONID === ID && value.CLIENTTIMESTAMP.includes(day)) ||
        (value.FIRSTINTENT === ID && value.CLIENTTIMESTAMP.includes(day))
      );
    else return value.CONVERSATIONID === ID || value.FIRSTINTENT === ID;
  });

  return (
    <DataTable
      rows={filtered}
      headers={headerData}
      useZebraStyles
      stickyHeader
      size="compact"
    >
      {({
        rows,
        headers,
        getHeaderProps,
        getTableProps,
        getTableContainerProps,
      }) => (
        <TableContainer
          title={`${textLanguage[language].infoTable.group}: ${
            ID === "" ? "-" : ID
          }`}
          {...getTableContainerProps()}
        >
          <TableToolbar>
            <TableToolbarContent>
              <Button
                onClick={() => {
                  sendScore(
                    conversations,
                    credentialsAndDefaults.credentials.connectionString,
                    credentialsAndDefaults.defaults.logsTable,
                    setConversations
                  );
                }}
              >
                {textLanguage[language].infoTable.sendButton}
              </Button>
            </TableToolbarContent>
          </TableToolbar>
          <Table {...getTableProps()}>
            <TableHead>
              <TableRow>
                {headers.map((header) => (
                  <TableHeader {...getHeaderProps({ header })}>
                    {header.header}
                  </TableHeader>
                ))}
              </TableRow>
            </TableHead>
            <TableBody>
              {rows.map((row) => (
                <TableRow key={row.id}>
                  {row.cells.map((cell) => (
                    <TableCell key={cell.id}>{cell.value}</TableCell>
                  ))}
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      )}
    </DataTable>
  );
}
