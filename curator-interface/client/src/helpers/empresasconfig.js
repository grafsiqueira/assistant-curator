import LogoIBM from "../images/logoIBM.png";

const companies = {
  ibm: {
    mainColor: "#4A82C3",
    logo: LogoIBM,
  },
};

export function getLogo(company) {
  return companies[company] ? companies[company].logo : companies.ibm.logo;
}

export function getMainColor(company) {
  return companies[company]
    ? companies[company].mainColor
    : companies.ibm.mainColor;
}
