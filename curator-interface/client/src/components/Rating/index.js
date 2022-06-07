import { useGlobalState } from "../../hooks/globalState";
import PropTypes from "prop-types";
import Rating from "@mui/material/Rating";
import SentimentVeryDissatisfiedIcon from "@mui/icons-material/SentimentVeryDissatisfied";
import SentimentDissatisfiedIcon from "@mui/icons-material/SentimentDissatisfied";
import SentimentSatisfiedIcon from "@mui/icons-material/SentimentSatisfied";
import SentimentSatisfiedAltIcon from "@mui/icons-material/SentimentSatisfiedAltOutlined";
import SentimentVerySatisfiedIcon from "@mui/icons-material/SentimentVerySatisfied";
import { createRows } from "../../helpers/helpers";

const customIcons = {
  1: {
    icon: <SentimentVeryDissatisfiedIcon />,
    label: "Very Dissatisfied",
  },
  2: {
    icon: <SentimentDissatisfiedIcon />,
    label: "Dissatisfied",
  },
  3: {
    icon: <SentimentSatisfiedIcon />,
    label: "Neutral",
  },
  4: {
    icon: <SentimentSatisfiedAltIcon />,
    label: "Satisfied",
  },
  5: {
    icon: <SentimentVerySatisfiedIcon />,
    label: "Very Satisfied",
  },
};

function IconContainer(props) {
  const { value, ...other } = props;
  return <span {...other}>{customIcons[value].icon}</span>;
}

IconContainer.propTypes = {
  value: PropTypes.number.isRequired,
};

export default function BasicRating({ conversationID, logID, defaultValue }) {
  const { conversations, setConversations, setRowData } = useGlobalState();

  function updateScore(newValue) {
    conversations[conversationID].map((log) => {
      if (log.LOGID === logID) {
        log.SCORE = newValue;
        createRows(conversations, setRowData);
      }
    });
  }

  return (
    <Rating
      name="highlight-selected-only"
      defaultValue={defaultValue}
      IconContainerComponent={IconContainer}
      highlightSelectedOnly
      sx={{
        "& .MuiRating-iconFilled": {
          color: "#0E61FE",
        },
        "& .MuiRating-iconEmpty": {
          color: "#b5b5b5",
        },
      }}
      onChange={(e, newValue) => {
        updateScore(newValue);
      }}
    />
  );
}
