import React, { useEffect } from "react";
import { Pagination } from "carbon-components-react";
import Languages from "../../helpers/languagesConfig";
import { useGlobalState } from "../../hooks/globalState";

export default function PaginationComponent() {
  const {
    conversationsByDay,
    dateFilter,
    itensPerPage,
    setItensPerPage,
    currentPage,
    setCurrentPage,
    language,
  } = useGlobalState();

  function selectFirstTab() {
    const firstTab = document.getElementById("firstTab");
    if (firstTab) firstTab.click();
  }

  return (
    <footer id="pagination">
      <Pagination
        itemsPerPageText={Languages[language].tabs.itensPerPage}
        page={currentPage}
        pageNumberText="Page Number"
        pageSize={itensPerPage}
        pageSizes={[9]}
        totalItems={conversationsByDay[dateFilter].length}
        onChange={(data) => {
          setCurrentPage(data.page);
          setItensPerPage(data.pageSize);
          selectFirstTab();
        }}
      />
    </footer>
  );
}
