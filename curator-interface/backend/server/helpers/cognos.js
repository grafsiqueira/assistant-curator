const axios = require("axios");

async function getCognosSession(username, password) {
  const options = {
    method: "POST",
    url: "https://dde-us-south.analytics.ibm.com/daas/v1/session",
    headers: {
      "Content-Type": "application/json",
    },
    auth: {
      username: username,
      password: password,
    },
  };

  try {
    const response = await axios.request(options);
    return response.data.sessionCode;
  } catch (err) {
    return {
      Error:
        "Was not able to connect to a Cognos Embedded Instance with the supplied credentials.",
    };
  }
}

function initializeDashboard(
  xsd,
  jdbcUrl,
  driverClassName,
  schema,
  user,
  password,
  logsTable,
  conversationTable,
  callsTable,
  contextTable,
  conversationPathTable,
  overviewTable,
  classDistributionTable,
  precisionAtKTable,
  classAccuracyTable,
  pairWiseClassErrorsTable,
  accuracyVsCoverageTable
) {
  const standardDashboard = {
    name: "Standard dashboard",
    layout: {
      id: "model0000017e7cdcdd61_00000000",
      items: [
        {
          id: "model0000017eb1760c6f_00000000",
          items: [
            {
              id: "model0000017eb1760c6f_00000001",
              css: "templateBox aspectRatio_default",
              items: [
                {
                  id: "model0000017eb1760c6f_00000002",
                  style: {
                    top: "0%",
                    left: "0%",
                    right: "50%",
                    bottom: "75%",
                  },
                  type: "templateDropZone",
                  templateName: "dz1",
                  relatedLayouts: "",
                },
                {
                  id: "model0000017eb1760c70_00000000",
                  css: "noBorderLeft",
                  style: {
                    top: "0%",
                    left: "50%",
                    right: "0%",
                    bottom: "75%",
                  },
                  type: "templateDropZone",
                  templateName: "dz3",
                  relatedLayouts: "",
                },
                {
                  id: "model0000017eb1760c70_00000001",
                  css: "noBorderTop",
                  style: {
                    top: "25%",
                    left: "0%",
                    right: "0%",
                    bottom: "0%",
                  },
                  type: "templateDropZone",
                  templateName: "dz2",
                },
                {
                  id: "model0000017eb17e688f_00000000",
                  style: {
                    left: "0%",
                    top: "24.39826407450131%",
                    height: "19.99751565405082%",
                    width: "99.97445529676935%",
                    opacity: 0.66,
                  },
                  type: "widget",
                  relatedLayouts: "",
                  content: {},
                },
                {
                  id: "model0000017eb1766a05_00000003",
                  style: {
                    left: "63.93688955672427%",
                    top: "1.2021028786356933%",
                    height: "23.797897121364308%",
                    width: "35.22960180315552%",
                  },
                  type: "widget",
                  relatedLayouts: "",
                  content: {},
                },
                {
                  id: "model0000017eb1773a05_00000004",
                  style: {
                    left: "0.976709241172051%",
                    top: "44.61137349603573%",
                    height: "53.105432436475546%",
                    width: "37.10323065364388%",
                  },
                  type: "widget",
                  relatedLayouts: "",
                  content: {},
                },
                {
                  id: "model0000017eb1784e05_00000000",
                  style: {
                    left: "71.14951164537942%",
                    top: "44.74494048255081%",
                    height: "53.39968050776826%",
                    width: "27.852667167543203%",
                  },
                  type: "widget",
                  relatedLayouts: "",
                  content: {},
                },
                {
                  id: "model0000017eb17b2fc4_00000000",
                  style: {
                    left: "1.051840721262209%",
                    top: "37.265189237706494%",
                    height: "7.127054260252603%",
                    width: "37.01517655897822%",
                    opacity: 1,
                  },
                  type: "widget",
                  relatedLayouts: "",
                  content: {},
                },
                {
                  id: "model0000017eb17c87dd_00000000",
                  style: {
                    left: "0.976709241172051%",
                    top: "25.110593464834484%",
                    height: "7.403577992434765%",
                    width: "31.13380916604057%",
                  },
                  type: "widget",
                  relatedLayouts: "",
                  content: {},
                },
                {
                  id: "model0000017eb17d1e22_00000000",
                  style: {
                    left: "35.01483679525222%",
                    top: "25.321441634077033%",
                    height: "7.116169147230116%",
                    width: "28.075568743818%",
                  },
                  type: "widget",
                  relatedLayouts: "",
                  content: {},
                },
                {
                  id: "model0000017eb17d5a7c_00000000",
                  style: {
                    left: "67.16754320060105%",
                    top: "24.84345949180433%",
                    height: "7.670711965464919%",
                    width: "31.831705484598046%",
                  },
                  type: "widget",
                  relatedLayouts: "",
                  content: {},
                },
                {
                  id: "model0000017eb17e0f18_00000000",
                  style: {
                    left: "71.14951164537942%",
                    top: "37.39875622422157%",
                    height: "7.120789968585045%",
                    width: "27.92546957175056%",
                    opacity: 0.87,
                  },
                  type: "widget",
                  relatedLayouts: "",
                  content: {},
                },
                {
                  id: "model0000017eb17a6940_00000003",
                  style: {
                    left: "0.9015777610818934%",
                    top: "1.4692368516658474%",
                    height: "23.488956681554935%",
                    width: "32.259579263711494%",
                  },
                  type: "widget",
                  relatedLayouts: "",
                  content: {},
                },
                {
                  id: "model0000017eb1763297_00000003",
                  style: {
                    left: "31.930879038317055%",
                    top: "1.3356698651507704%",
                    height: "23.77185155899387%",
                    width: "35.224943651389935%",
                  },
                  type: "widget",
                  relatedLayouts: "",
                  content: {},
                },
                {
                  id: "model0000017ec0fdf257_00000002",
                  style: {
                    left: "39.51915852742299%",
                    top: "44.61137349603573%",
                    height: "53.12840595815614%",
                    width: "30.45056348610068%",
                  },
                  type: "widget",
                  relatedLayouts: "",
                  content: {},
                },
                {
                  id: "model0000017ec0feae2a_00000000",
                  style: {
                    left: "39.44402704733283%",
                    top: "37.131622251191416%",
                    height: "7.302360930053641%",
                    width: "30.531555221637866%",
                  },
                  type: "widget",
                  relatedLayouts: "",
                  content: {},
                },
              ],
              type: "genericPage",
              layoutPositioning: "relative",
            },
          ],
          type: "container",
          title: {
            translationTable: {
              Default: "Visão Geral",
            },
          },
          templateName: "Template3",
        },
        {
          id: "model0000017e7cdcdd61_00000001",
          items: [
            {
              id: "model0000017e7cdcdd61_00000002",
              css: "templateBox aspectRatio_default",
              items: [
                {
                  id: "model0000017e7cdcdd61_00000003",
                  style: {
                    top: "0%",
                    left: "0%",
                    right: "0%",
                    bottom: "0%",
                  },
                  type: "templateIndicator",
                },
                {
                  id: "model0000017e8d7e356e_00000000",
                  style: {
                    left: "41.92336589030804%",
                    top: "40.87149787361358%",
                    height: "28.211885324728055%",
                    width: "57.079939894815936%",
                  },
                  type: "widget",
                  relatedLayouts: "",
                  content: {},
                },
                {
                  id: "model0000017e9c99bcde_00000000",
                  style: {
                    left: "41.848234410217884%",
                    top: "72.92757463723207%",
                    height: "26.10820528711559%",
                    width: "57.065890308039066%",
                  },
                  type: "widget",
                  relatedLayouts: "",
                  content: {},
                },
                {
                  id: "model0000017ea15b1b5e_00000000",
                  style: {
                    left: "2.9301277235161534%",
                    top: "41.806466779219114%",
                    height: "56.156636676425954%",
                    width: "32.099924868519906%",
                  },
                  type: "widget",
                  relatedLayouts: "",
                  content: {},
                },
                {
                  id: "model0000017eb1823fd5_00000000",
                  style: {
                    left: "0.9015777610818934%",
                    top: "25.24416045134956%",
                    height: "7.110358386938218%",
                    width: "32.070022539444025%",
                  },
                  type: "widget",
                  relatedLayouts: "",
                  content: {},
                },
                {
                  id: "model0000017eb18349fb_00000002",
                  style: {
                    left: "33.13298271975958%",
                    top: "1.3356698651507704%",
                    height: "23.66433013484923%",
                    width: "32.85890308039068%",
                  },
                  type: "widget",
                  relatedLayouts: "",
                  content: {},
                },
                {
                  id: "model0000017eb18597dc_00000000",
                  style: {
                    left: "33.20811419984974%",
                    top: "25.24416045134956%",
                    height: "7.135402196909795%",
                    width: "31.10563486100676%",
                  },
                  type: "widget",
                  relatedLayouts: "",
                  content: {},
                },
                {
                  id: "model0000017eb1859b57_00000000",
                  style: {
                    left: "64.31254695717506%",
                    top: "25.110593464834484%",
                    height: "7.403577992434765%",
                    width: "34.85867768595041%",
                  },
                  type: "widget",
                  relatedLayouts: "",
                  content: {},
                },
                {
                  id: "model0000017eb1897146_00000000",
                  style: {
                    left: "0.05860255447032449%",
                    top: "34.19314854785972%",
                    height: "7.508975701493814%",
                    width: "37.955522163786625%",
                  },
                  type: "widget",
                  relatedLayouts: "",
                  content: {},
                },
                {
                  id: "model0000017eb19252a9_00000000",
                  style: {
                    left: "41.92336589030804%",
                    top: "34.460282520889876%",
                    height: "7.308625221721198%",
                    width: "57.10698722764839%",
                  },
                  type: "widget",
                  relatedLayouts: "",
                  content: {},
                },
                {
                  id: "model0000017eb192cbb7_00000000",
                  style: {
                    left: "41.92336589030804%",
                    top: "67.45132819011391%",
                    height: "7.375408714978736%",
                    width: "57.23846731780617%",
                  },
                  type: "widget",
                  relatedLayouts: "",
                  content: {},
                },
                {
                  id: "model0000017ec0f7adef_00000003",
                  style: {
                    left: "0.9015777610818934%",
                    top: "1.6028038381809244%",
                    height: "23.488021712649328%",
                    width: "32.13688955672427%",
                  },
                  type: "widget",
                  relatedLayouts: "",
                  content: {},
                },
                {
                  id: "model0000017eb1833152_00000003",
                  style: {
                    left: "65.89030803906837%",
                    top: "1.3356698651507704%",
                    height: "23.838635052251405%",
                    width: "33.19346356123216%",
                  },
                  type: "widget",
                  relatedLayouts: "",
                  content: {},
                },
                {
                  id: "model0000017ec0fab020_00000000",
                  style: {
                    left: "0%",
                    top: "24.39826407450131%",
                    height: "19.99751565405082%",
                    width: "99.97445529676935%",
                    opacity: 0.66,
                  },
                  type: "widget",
                  relatedLayouts: "",
                  content: {},
                },
              ],
              type: "genericPage",
              layoutPositioning: "relative",
            },
          ],
          type: "container",
          title: {
            translationTable: {
              Default: "Dados secundários",
            },
          },
          templateName: "Template1",
        },
        {
          id: "model000001804846717c_00000000",
          items: [
            {
              id: "model000001804846717c_00000001",
              css: "templateBox aspectRatio_default",
              items: [
                {
                  id: "model000001804846717c_00000002",
                  style: {
                    top: "0%",
                    left: "0%",
                    right: "0%",
                    bottom: "0%",
                  },
                  type: "templateIndicator",
                },
                {
                  id: "model0000018048468cea_00000002",
                  style: {
                    left: "38.01652892561984%",
                    top: "0%",
                    height: "32.33776953817879%",
                    width: "27.177610818933132%",
                  },
                  type: "widget",
                  relatedLayouts: "",
                  content: {},
                },
                {
                  id: "model00000180484a631c_00000000",
                  style: {
                    left: "0.976709241172051%",
                    top: "28.449768127711412%",
                    height: "55.3259835872887%",
                    width: "97.7092411720511%",
                  },
                  type: "widget",
                  relatedLayouts: "",
                  content: {},
                },
              ],
              type: "genericPage",
              layoutPositioning: "relative",
            },
          ],
          type: "container",
          title: {
            translationTable: {
              Default: "Chamadas",
            },
          },
          templateName: "Template1",
        },
        {
          id: "model0000017fd1d868b1_00000000",
          items: [
            {
              id: "model0000017fd1d868b2_00000000",
              css: "templateBox aspectRatio_default",
              items: [
                {
                  id: "model0000017fd1d868b2_00000001",
                  style: {
                    top: "0%",
                    left: "0%",
                    right: "0%",
                    bottom: "0%",
                  },
                  type: "templateIndicator",
                },
                {
                  id: "model0000017fff5cd317_00000000",
                  style: {
                    left: "0.8172362555720654%",
                    top: "1.4528644543503384%",
                    height: "48.463595839524515%",
                    width: "98.2332838038633%",
                  },
                  type: "widget",
                  relatedLayouts: "",
                  content: {},
                },
                {
                  id: "model0000017fff5e4fe8_00000000",
                  style: {
                    left: "0.8172362555720654%",
                    top: "49.26531286115239%",
                    height: "50.20637279181113%",
                    width: "98.19985141158989%",
                  },
                  type: "widget",
                  relatedLayouts: "",
                  content: {},
                },
              ],
              type: "genericPage",
              layoutPositioning: "relative",
            },
          ],
          type: "container",
          title: {
            translationTable: {
              Default: "Nós de Diálogo",
            },
          },
          templateName: "Template1",
        },
        {
          id: "model0000017fd10deb88_00000000",
          items: [
            {
              id: "model0000017fd10deb88_00000001",
              css: "templateBox aspectRatio_default",
              items: [
                {
                  id: "model0000017fd10deb88_00000002",
                  style: {
                    top: "0%",
                    left: "0%",
                    right: "0%",
                    bottom: "0%",
                  },
                  type: "templateIndicator",
                },
                {
                  id: "model0000017fff620981_00000000",
                  style: {
                    left: "0.8172362555720654%",
                    top: "1.4528644543503384%",
                    height: "48.463595839524515%",
                    width: "98.2332838038633%",
                  },
                  type: "widget",
                  relatedLayouts: "",
                  content: {},
                },
                {
                  id: "model0000017fff62a8d5_00000006",
                  style: {
                    left: "0.8915304606240714%",
                    top: "49.79362720818887%",
                    height: "46.411226679874524%",
                    width: "98.19985141158989%",
                  },
                  type: "widget",
                  relatedLayouts: "",
                  content: {},
                },
              ],
              type: "genericPage",
              layoutPositioning: "relative",
            },
          ],
          type: "container",
          title: {
            translationTable: {
              Default: "Intenções",
            },
          },
          templateName: "Template1",
        },
        {
          id: "model0000017fd6818109_00000000",
          items: [
            {
              id: "model0000017fd6818109_00000001",
              css: "templateBox aspectRatio_default",
              items: [
                {
                  id: "model0000017fd6818109_00000002",
                  style: {
                    top: "0%",
                    left: "0%",
                    right: "0%",
                    bottom: "0%",
                  },
                  type: "templateIndicator",
                },
                {
                  id: "model0000017fff6e7a2d_00000000",
                  style: {
                    left: "26.06382978723404%",
                    top: "1.485984464707869%",
                    height: "75.22485646740965%",
                    width: "72.99285714285715%",
                  },
                  type: "widget",
                  relatedLayouts: "",
                  content: {},
                },
                {
                  id: "model00000180055d02b8_00000002",
                  style: {
                    left: "0.9878419452887538%",
                    top: "1.7561634582911179%",
                    height: "74.8079702803107%",
                    width: "25.124088145896653%",
                  },
                  type: "widget",
                  relatedLayouts: "",
                  content: {},
                },
              ],
              type: "genericPage",
              layoutPositioning: "relative",
            },
          ],
          type: "container",
          title: {
            translationTable: {
              Default: "Caminhos da Conversa",
            },
          },
          templateName: "Template1",
        },
        {
          id: "model0000017fff122c8f_00000000",
          items: [
            {
              id: "model0000017fff122c8f_00000001",
              css: "templateBox aspectRatio_default",
              items: [
                {
                  id: "model0000017fff122c90_00000000",
                  style: {
                    top: "0%",
                    left: "0%",
                    right: "0%",
                    bottom: "0%",
                  },
                  type: "templateIndicator",
                },
                {
                  id: "model0000017fff18ef93_00000000",
                  style: {
                    left: "1%",
                    top: "0.3465453757851419%",
                    height: "49.86320121290882%",
                    width: "97.31510721247564%",
                  },
                  type: "widget",
                  relatedLayouts: "",
                  content: {},
                },
                {
                  id: "model0000017fff447a44_00000000",
                  style: {
                    left: "0.8771929824561403%",
                    top: "46.95689841888672%",
                    height: "49.98241282217891%",
                    width: "97.45146198830409%",
                  },
                  type: "widget",
                  relatedLayouts: "",
                  content: {},
                },
              ],
              type: "genericPage",
              layoutPositioning: "relative",
            },
          ],
          type: "container",
          title: {
            translationTable: {
              Default: "Distribuição de classes",
            },
          },
          templateName: "Template1",
        },
        {
          id: "model0000017fff1b8309_00000000",
          items: [
            {
              id: "model0000017fff1b830a_00000000",
              css: "templateBox aspectRatio_default",
              items: [
                {
                  id: "model0000017fff1b830a_00000001",
                  style: {
                    top: "0%",
                    left: "0%",
                    right: "0%",
                    bottom: "0%",
                  },
                  type: "templateIndicator",
                },
                {
                  id: "model0000017fff1b9dfc_00000004",
                  style: {
                    left: "0.9658246656760773%",
                    top: "1.58494304110946%",
                    height: "49.019778768367175%",
                    width: "98.2258543833581%",
                  },
                  type: "widget",
                  relatedLayouts: "",
                  content: {},
                },
                {
                  id: "model0000017fff45f3f8_00000000",
                  style: {
                    left: "0.8915304606240714%",
                    top: "48.20868416707941%",
                    height: "49.99795278190524%",
                    width: "98.35661218424963%",
                  },
                  type: "widget",
                  relatedLayouts: "",
                  content: {
                    properties: {
                      suppression: "rowsAndColumns",
                    },
                  },
                },
              ],
              type: "genericPage",
              layoutPositioning: "relative",
            },
          ],
          type: "container",
          title: {
            translationTable: {
              Default: "Precision @K",
            },
          },
          templateName: "Template1",
        },
        {
          id: "model0000017fff1c7e6b_00000000",
          items: [
            {
              id: "model0000017fff1c7e6b_00000001",
              css: "templateBox aspectRatio_default",
              items: [
                {
                  id: "model0000017fff1c7e6b_00000002",
                  style: {
                    top: "0%",
                    left: "0%",
                    right: "0%",
                    bottom: "0%",
                  },
                  type: "templateIndicator",
                },
                {
                  id: "model0000017fff1d32bf_00000000",
                  style: {
                    left: "0.8172362555720654%",
                    top: "1.58494304110946%",
                    height: "50.07019976886247%",
                    width: "98.44873699851412%",
                  },
                  type: "widget",
                  relatedLayouts: "",
                  content: {},
                },
                {
                  id: "model0000017fff48ec9b_00000000",
                  style: {
                    left: "0.9658246656760773%",
                    top: "51.64272742281658%",
                    height: "48.35727257718342%",
                    width: "98.33506686478455%",
                  },
                  type: "widget",
                  relatedLayouts: "",
                  content: {},
                },
              ],
              type: "genericPage",
              layoutPositioning: "relative",
            },
          ],
          type: "container",
          title: {
            translationTable: {
              Default: "Acurácia das Intenções",
            },
          },
          templateName: "Template1",
        },
        {
          id: "model0000017fff253ff5_00000000",
          items: [
            {
              id: "model0000017fff253ff5_00000001",
              css: "templateBox aspectRatio_default",
              items: [
                {
                  id: "model0000017fff253ff5_00000002",
                  style: {
                    top: "0%",
                    left: "0%",
                    right: "0%",
                    bottom: "0%",
                  },
                  type: "templateIndicator",
                },
                {
                  id: "model0000017fff2f930d_00000000",
                  style: {
                    left: "0.9658246656760773%",
                    top: "1.3207858675912167%",
                    height: "75.20950965824665%",
                    width: "98.10401188707282%",
                  },
                  type: "widget",
                  relatedLayouts: "",
                  content: {
                    properties: {
                      suppression: "rowsAndColumns",
                    },
                  },
                },
              ],
              type: "genericPage",
              layoutPositioning: "relative",
            },
          ],
          type: "container",
          title: {
            translationTable: {
              Default: "Intenções confundidas",
            },
          },
          templateName: "Template1",
        },
        {
          id: "model0000017fff34aff7_00000000",
          items: [
            {
              id: "model0000017fff34aff7_00000001",
              css: "templateBox aspectRatio_default",
              items: [
                {
                  id: "model0000017fff34aff7_00000002",
                  style: {
                    top: "0%",
                    left: "0%",
                    right: "0%",
                    bottom: "0%",
                  },
                  type: "templateIndicator",
                },
                {
                  id: "model0000017fff37764e_00000000",
                  style: {
                    left: "0.9658246656760773%",
                    top: "0%",
                    height: "66.58623080733037%",
                    width: "98.14858841010401%",
                  },
                  type: "widget",
                  relatedLayouts: "",
                  content: {},
                },
              ],
              type: "genericPage",
              layoutPositioning: "relative",
            },
          ],
          type: "container",
          title: {
            translationTable: {
              Default: "Acurácia vs. Cobertura",
            },
          },
          templateName: "Template1",
        },
        {
          id: "model0000017fff3b9e4d_00000000",
          items: [
            {
              id: "model0000017fff3b9e4e_00000000",
              css: "templateBox aspectRatio_default",
              items: [
                {
                  id: "model0000017fff3b9e4e_00000001",
                  style: {
                    top: "0%",
                    left: "0%",
                    right: "0%",
                    bottom: "0%",
                  },
                  type: "templateIndicator",
                },
                {
                  id: "model0000017fff3ebe85_00000002",
                  style: {
                    left: "0.9658246656760773%",
                    top: "1.3207858675912167%",
                    height: "53.87670463926036%",
                    width: "98.10401188707282%",
                  },
                  type: "widget",
                  relatedLayouts: "",
                  content: {
                    properties: {
                      suppression: "rowsAndColumns",
                    },
                  },
                },
              ],
              type: "genericPage",
              layoutPositioning: "relative",
            },
          ],
          type: "container",
          title: {
            translationTable: {
              Default: "Previsões",
            },
          },
          templateName: "Template1",
        },
      ],
      style: {
        height: "100%",
      },
      type: "tab",
      showGrid: false,
      snapGrid: true,
      snapObjects: true,
      pageSize: {
        width: 1280,
        height: 720,
      },
      layoutPositioning: "relative",
      fitPage: false,
      content: {},
    },
    theme: "defaultTheme",
    version: 1601,
    eventGroups: [
      {
        id: "model0000017e7cdcdd61_00000001:1",
        widgetIds: [
          "model0000017e8d7e356e_00000000",
          "model0000017e9c99bcde_00000000",
          "model0000017ea15b1b5e_00000000",
          "model0000017eb1823fd5_00000000",
          "model0000017eb1833152_00000003",
          "model0000017eb18349fb_00000002",
          "model0000017eb1859b57_00000000",
          "model0000017eb18597dc_00000000",
          "model0000017eb1897146_00000000",
          "model0000017eb19252a9_00000000",
          "model0000017eb192cbb7_00000000",
          "model0000017ec0f7adef_00000003",
        ],
      },
      {
        id: "model0000017eb1760c6f_00000000:1",
        widgetIds: [
          "model0000017eb1763297_00000003",
          "model0000017eb1766a05_00000003",
          "model0000017eb1773a05_00000004",
          "model0000017eb1784e05_00000000",
          "model0000017eb17a6940_00000003",
          "model0000017eb17b2fc4_00000000",
          "model0000017eb17c87dd_00000000",
          "model0000017eb17d1e22_00000000",
          "model0000017eb17d5a7c_00000000",
          "model0000017eb17e0f18_00000000",
          "model0000017eb17e688f_00000000",
          "model0000017ec0fdf257_00000002",
          "model0000017ec0feae2a_00000000",
        ],
      },
      {
        id: "model0000017fff122c8f_00000000:1",
        widgetIds: [
          "model0000017fff18ef93_00000000",
          "model0000017fff447a44_00000000",
        ],
      },
      {
        id: "model0000017fff1b8309_00000000:1",
        widgetIds: [
          "model0000017fff1b9dfc_00000004",
          "model0000017fff45f3f8_00000000",
        ],
      },
      {
        id: "model0000017fff1c7e6b_00000000:1",
        widgetIds: [
          "model0000017fff1d32bf_00000000",
          "model0000017fff48ec9b_00000000",
        ],
      },
      {
        id: "model0000017fff253ff5_00000000:1",
        widgetIds: ["model0000017fff2f930d_00000000"],
      },
      {
        id: "model0000017fff34aff7_00000000:1",
        widgetIds: ["model0000017fff37764e_00000000"],
      },
      {
        id: "model0000017fff3b9e4d_00000000:1",
        widgetIds: ["model0000017fff3ebe85_00000002"],
      },
      {
        id: "model0000017fd1d868b1_00000000:1",
        widgetIds: [
          "model0000017fff5cd317_00000000",
          "model0000017fff5e4fe8_00000000",
        ],
      },
      {
        id: "model0000017fd10deb88_00000000:1",
        widgetIds: [
          "model0000017fff620981_00000000",
          "model0000017fff62a8d5_00000006",
        ],
      },
      {
        id: "model0000017fd6818109_00000000:1",
        widgetIds: [
          "model0000017fff6e7a2d_00000000",
          "model00000180055d02b8_00000002",
        ],
      },
      {
        id: "model000001804846717c_00000000:1",
        widgetIds: [
          "model0000018048468cea_00000002",
          "model00000180484a631c_00000000",
        ],
      },
    ],
    properties: {
      customColors: {
        colors: ["#1D2473"],
      },
      defaultLocale: "Default",
      fredIsRed: true,
    },
    drillThrough: [],
    fredIsRed: {
      id: "model0000017e7cdcdd60_00000000",
      colorMap: {
        0: {
          v: 0,
          s: 45,
        },
        1: {
          v: 18,
          s: 51,
        },
        2: {
          v: 17,
          s: 51,
        },
        3: {
          v: 16,
          s: 51,
        },
        4: {
          v: 15,
          s: 51,
        },
        6: {
          v: 41,
          s: 18,
        },
        8: {
          v: 1,
          s: 22,
        },
        10: {
          v: 2,
          s: 22,
        },
        16: {
          v: 3,
          s: 22,
        },
        17: {
          v: 4,
          s: 22,
        },
        20: {
          v: 5,
          s: 22,
        },
        21: {
          v: 6,
          s: 22,
        },
        23: {
          v: 7,
          s: 22,
        },
        26: {
          v: 8,
          s: 22,
        },
        27: {
          v: 11,
          s: 22,
        },
        28: {
          v: 9,
          s: 22,
        },
        30: {
          v: 10,
          s: 22,
        },
        34: {
          v: 40,
          s: 22,
        },
        37: {
          v: 10,
          s: 22,
        },
        50: {
          v: 14,
          s: 22,
        },
        51: {
          v: 13,
          s: 22,
        },
        52: {
          v: 11,
          s: 22,
        },
        57: {
          v: 12,
          s: 22,
        },
        58: {
          v: 11,
          s: 22,
        },
        60: {
          v: 12,
          s: 22,
        },
        69: {
          v: 13,
          s: 22,
        },
        88: {
          v: 10,
          s: 22,
        },
        89: {
          v: 9,
          s: 22,
        },
        115: {
          v: 8,
          s: 22,
        },
        157: {
          v: 14,
          s: 22,
        },
        204: {
          v: 15,
          s: 22,
        },
        235: {
          v: 7,
          s: 22,
        },
        3059: {
          v: 16,
          s: 7,
        },
        informar_falta_energia: {
          v: 17,
          s: 50,
        },
        transferir_atendimento_humano: {
          v: 18,
          s: 50,
        },
        religar_fornecimento_energia: {
          v: 19,
          s: 50,
        },
        pagar_conta_atrasada: {
          v: 20,
          s: 50,
        },
        solicitar_debito_automatico: {
          v: 21,
          s: 50,
        },
        negativo: {
          v: 22,
          s: 50,
        },
        xingamento: {
          v: 23,
          s: 50,
        },
        afirmativo: {
          v: 24,
          s: 50,
        },
        "(no value)": {
          v: 25,
          s: 47,
        },
        "Conversation Channel (Count distinct)": {
          v: 26,
          s: 11,
        },
        "ID Conversation (Count distinct)": {
          v: 27,
          s: 11,
        },
        "Chat Count (Count distinct)": {
          v: 28,
          s: 51,
        },
        "Phone Count (Count distinct)": {
          v: 29,
          s: 51,
        },
        "reccurrentUsers (Calculated)": {
          v: 30,
          s: 21,
        },
        "newUser (Calculated)": {
          v: 31,
          s: 21,
        },
        "false,false,false,true,false": {
          v: 32,
          s: 18,
        },
        "true,false,false,false,false": {
          v: 33,
          s: 18,
        },
        "false,false,true,false": {
          v: 34,
          s: 18,
        },
        "false,false,false,false": {
          v: 35,
          s: 18,
        },
        "false,true,false": {
          v: 36,
          s: 18,
        },
        "false,false,false": {
          v: 37,
          s: 18,
        },
        "true,false": {
          v: 38,
          s: 18,
        },
        "false,false": {
          v: 39,
          s: 18,
        },
        false: {
          v: 40,
          s: 18,
        },
        "noFeedback (Sum)": {
          v: 42,
          s: 21,
        },
        "perfectFeedback (Sum)": {
          v: 43,
          s: 21,
        },
        "goodFeedback (Sum)": {
          v: 44,
          s: 21,
        },
        "mediumFeedback (Sum)": {
          v: 45,
          s: 21,
        },
        "badFeedback (Sum)": {
          v: 46,
          s: 21,
        },
        "No Feedback (Sum)": {
          v: 47,
          s: 51,
        },
        "1 - 3  (Sum)": {
          v: 0,
          s: 51,
        },
        "4 - 6 (Sum)": {
          v: 1,
          s: 51,
        },
        "7 - 9 (Sum)": {
          v: 2,
          s: 51,
        },
        "10 (Sum)": {
          v: 3,
          s: 51,
        },
        "Recurrent Users (Calculated)": {
          v: 4,
          s: 51,
        },
        "New Users (Calculated)": {
          v: 5,
          s: 51,
        },
        "3,059": {
          v: 6,
          s: 22,
        },
        "b9da2476-fdfb-40bd-926f-9034feb8bf85": {
          v: 19,
          s: 22,
        },
        "4d62d1f9-6647-4fe3-b2c4-8c04c26d5870": {
          v: 20,
          s: 22,
        },
        "a5829d98-fc16-45bf-8aff-9c755f716aac": {
          v: 21,
          s: 22,
        },
        "35935349-95e3-405a-9e71-a911772b9e5a": {
          v: 22,
          s: 22,
        },
        "49e4d834-8c8c-49e2-bfb4-3f737e292f95": {
          v: 23,
          s: 22,
        },
        "fd41f3b2-3045-4342-a560-b96b0b846e21": {
          v: 24,
          s: 22,
        },
        "582db031-3f4b-4c57-b363-2aeefc963b2e": {
          v: 25,
          s: 22,
        },
        "86f0093e-2320-41be-a827-f753746024c5": {
          v: 26,
          s: 22,
        },
        "8aa1f4cd-6c0d-4a4f-8a38-dc08aaaeecf5": {
          v: 27,
          s: 22,
        },
        "0c5540ba-eb94-4454-88d6-6dad40691feb": {
          v: 28,
          s: 22,
        },
        "e7aaedab-c725-487f-8e7e-c1fcacd10271": {
          v: 29,
          s: 22,
        },
        "10ba1156-9a35-4736-9a93-e52301d3069a": {
          v: 30,
          s: 22,
        },
        "32109400-3893-4a6b-8527-d8ac4e7d6df0": {
          v: 31,
          s: 22,
        },
        "7d8f89c3-59d4-4b9c-b362-3f37da7ecec7": {
          v: 32,
          s: 22,
        },
        "1e8e14a9-8234-4afc-93af-2d98da4992b0": {
          v: 33,
          s: 22,
        },
        "a5f83e3e-a8f2-46a9-8f06-87527e8d9824": {
          v: 34,
          s: 22,
        },
        "b73eff78-b966-4c36-865c-945e66523908": {
          v: 35,
          s: 22,
        },
        "305eb1d5-c58e-4e08-907c-23f6beede328": {
          v: 36,
          s: 22,
        },
        "4e9e7b76-1712-419c-9920-1f4cdd98af89": {
          v: 37,
          s: 22,
        },
        "7df40d1b-4330-4702-8acf-1a94035cb20f": {
          v: 38,
          s: 22,
        },
        "90c444e7-fef2-4c4e-8d90-8872d97c077b": {
          v: 39,
          s: 22,
        },
        "c2473fcd-fba4-4aff-912b-2857c77b45a3": {
          v: 40,
          s: 22,
        },
        "afe3e888-0d48-4114-bb3f-3563180ecbfe": {
          v: 41,
          s: 22,
        },
        "4b0e7c63-857a-483f-9444-8314042f063b": {
          v: 42,
          s: 22,
        },
        "1a8fa8e9-9437-45d8-8fa4-ebc12242e9c0": {
          v: 43,
          s: 22,
        },
        "03ea84d0-6a9b-471a-9abc-c9c03372485b": {
          v: 44,
          s: 22,
        },
        "56c29b8e-95c7-4af2-859b-7f5c8e277e1e": {
          v: 45,
          s: 22,
        },
        "b64503a5-a488-4712-8aa5-8422df4f60de": {
          v: 46,
          s: 22,
        },
        "cb4468fe-79b0-47ba-923f-86d04cd0aac5": {
          v: 47,
          s: 22,
        },
        "ae47ddef-11a9-44e0-9f44-2e35ba05e8c7": {
          v: 0,
          s: 22,
        },
        "7fd53a3b-fc9e-4ed7-91ea-f0b67b63046f": {
          v: 1,
          s: 22,
        },
        "c90b11f4-58ce-450a-a726-0756394cb295": {
          v: 2,
          s: 22,
        },
        "507dfd83-379e-49e6-84db-d3014de112ad": {
          v: 3,
          s: 22,
        },
        "4350c732-1b16-4232-9592-6ca1f2ef807b": {
          v: 4,
          s: 22,
        },
        "72e72a5e-4c99-4ba4-9715-eb4339e080ca": {
          v: 5,
          s: 22,
        },
        "cc7df136-3c02-40f6-9e0c-03b858415491": {
          v: 6,
          s: 22,
        },
        "2874f932-577c-461b-ba22-0a4e6192da05": {
          v: 7,
          s: 22,
        },
        Phone: {
          v: 8,
          s: 22,
        },
        Chat: {
          v: 9,
          s: 22,
        },
        "Phone Count (Count distinct),0": {
          v: 12,
          s: 22,
        },
        "Phone Count (Count distinct),8": {
          v: 13,
          s: 22,
        },
        "Phone Count (Count distinct),10": {
          v: 14,
          s: 22,
        },
        "Phone Count (Count distinct),16": {
          v: 15,
          s: 22,
        },
        "Phone Count (Count distinct),17": {
          v: 16,
          s: 22,
        },
        "Phone Count (Count distinct),20": {
          v: 17,
          s: 22,
        },
        "Phone Count (Count distinct),21": {
          v: 18,
          s: 22,
        },
        "Phone Count (Count distinct),23": {
          v: 19,
          s: 22,
        },
        "Phone Count (Count distinct),26": {
          v: 20,
          s: 22,
        },
        "Phone Count (Count distinct),28": {
          v: 21,
          s: 22,
        },
        "Phone Count (Count distinct),30": {
          v: 22,
          s: 22,
        },
        "Phone Count (Count distinct),50": {
          v: 23,
          s: 22,
        },
        "Phone Count (Count distinct),51": {
          v: 24,
          s: 22,
        },
        "Phone Count (Count distinct),52": {
          v: 25,
          s: 22,
        },
        "Phone Count (Count distinct),57": {
          v: 26,
          s: 22,
        },
        "Phone Count (Count distinct),58": {
          v: 27,
          s: 22,
        },
        "Phone Count (Count distinct),60": {
          v: 28,
          s: 22,
        },
        "Phone Count (Count distinct),69": {
          v: 29,
          s: 22,
        },
        "Phone Count (Count distinct),88": {
          v: 30,
          s: 22,
        },
        "Phone Count (Count distinct),89": {
          v: 31,
          s: 22,
        },
        "Phone Count (Count distinct),115": {
          v: 32,
          s: 22,
        },
        "Phone Count (Count distinct),157": {
          v: 33,
          s: 22,
        },
        "Phone Count (Count distinct),204": {
          v: 34,
          s: 22,
        },
        "Phone Count (Count distinct),235": {
          v: 35,
          s: 22,
        },
        "Phone Count (Count distinct),3,059": {
          v: 36,
          s: 22,
        },
        "Chat Count (Count distinct),0": {
          v: 37,
          s: 22,
        },
        "Chat Count (Count distinct),8": {
          v: 38,
          s: 22,
        },
        "Chat Count (Count distinct),10": {
          v: 39,
          s: 22,
        },
        "Chat Count (Count distinct),16": {
          v: 40,
          s: 22,
        },
        "Chat Count (Count distinct),17": {
          v: 41,
          s: 22,
        },
        "Chat Count (Count distinct),20": {
          v: 42,
          s: 22,
        },
        "Chat Count (Count distinct),21": {
          v: 43,
          s: 22,
        },
        "Chat Count (Count distinct),23": {
          v: 44,
          s: 22,
        },
        "Chat Count (Count distinct),26": {
          v: 45,
          s: 22,
        },
        "Chat Count (Count distinct),28": {
          v: 46,
          s: 22,
        },
        "Chat Count (Count distinct),30": {
          v: 47,
          s: 22,
        },
        "Chat Count (Count distinct),50": {
          v: 0,
          s: 22,
        },
        "Chat Count (Count distinct),51": {
          v: 1,
          s: 22,
        },
        "Chat Count (Count distinct),52": {
          v: 2,
          s: 22,
        },
        "Chat Count (Count distinct),57": {
          v: 3,
          s: 22,
        },
        "Chat Count (Count distinct),58": {
          v: 4,
          s: 22,
        },
        "Chat Count (Count distinct),60": {
          v: 5,
          s: 22,
        },
        "Chat Count (Count distinct),69": {
          v: 6,
          s: 22,
        },
        "Chat Count (Count distinct),88": {
          v: 7,
          s: 22,
        },
        "Chat Count (Count distinct),89": {
          v: 8,
          s: 22,
        },
        "Chat Count (Count distinct),115": {
          v: 9,
          s: 22,
        },
        "Chat Count (Count distinct),157": {
          v: 10,
          s: 22,
        },
        "Chat Count (Count distinct),204": {
          v: 11,
          s: 22,
        },
        "Chat Count (Count distinct),235": {
          v: 12,
          s: 22,
        },
        "Chat Count (Count distinct),3,059": {
          v: 13,
          s: 22,
        },
        "2,57": {
          v: 14,
          s: 22,
        },
        "1,3,059": {
          v: 15,
          s: 22,
        },
        "1,235": {
          v: 16,
          s: 22,
        },
        "1,204": {
          v: 17,
          s: 22,
        },
        "1,157": {
          v: 18,
          s: 22,
        },
        "1,115": {
          v: 19,
          s: 22,
        },
        "1,89": {
          v: 20,
          s: 22,
        },
        "1,88": {
          v: 21,
          s: 22,
        },
        "2,52": {
          v: 22,
          s: 22,
        },
        "2,51": {
          v: 23,
          s: 22,
        },
        "2,50": {
          v: 24,
          s: 22,
        },
        "3,16": {
          v: 25,
          s: 22,
        },
        "3,10": {
          v: 26,
          s: 22,
        },
        "3,8": {
          v: 27,
          s: 22,
        },
        "1,69": {
          v: 28,
          s: 22,
        },
        "2,30": {
          v: 29,
          s: 22,
        },
        "1,60": {
          v: 30,
          s: 22,
        },
        "2,28": {
          v: 31,
          s: 22,
        },
        "1,58": {
          v: 32,
          s: 22,
        },
        "2,26": {
          v: 33,
          s: 22,
        },
        "2,23": {
          v: 34,
          s: 22,
        },
        "3,20": {
          v: 35,
          s: 22,
        },
        "3,21": {
          v: 36,
          s: 22,
        },
        "4,0": {
          v: 37,
          s: 22,
        },
        "3,17": {
          v: 38,
          s: 22,
        },
        "3,0": {
          v: 39,
          s: 22,
        },
        General_Jokes: {
          v: 41,
          s: 51,
        },
        migrar_plano: {
          v: 42,
          s: 51,
        },
        cancelar_linha: {
          v: 43,
          s: 51,
        },
        General_Ending: {
          v: 44,
          s: 51,
        },
        General_Greetings: {
          v: 45,
          s: 51,
        },
        descrever_planos: {
          v: 46,
          s: 51,
        },
        consumo_voz: {
          v: 47,
          s: 51,
        },
        consumo_internet: {
          v: 0,
          s: 51,
        },
        Falha_de_Acesso: {
          v: 1,
          s: 51,
        },
        0.82: {
          v: 2,
          s: 45,
        },
        0.85: {
          v: 3,
          s: 45,
        },
        0.86: {
          v: 4,
          s: 45,
        },
        0.87: {
          v: 5,
          s: 45,
        },
        0.9: {
          v: 6,
          s: 45,
        },
        0.93: {
          v: 7,
          s: 45,
        },
        0.95: {
          v: 8,
          s: 45,
        },
        0.97: {
          v: 9,
          s: 45,
        },
        0.98: {
          v: 10,
          s: 45,
        },
        "0.82,1": {
          v: 11,
          s: 45,
        },
        "0.85,0.93": {
          v: 12,
          s: 45,
        },
        "0.86,0.86": {
          v: 13,
          s: 45,
        },
        "0.86,0.87": {
          v: 14,
          s: 45,
        },
        "0.87,0.86": {
          v: 15,
          s: 45,
        },
        "0.9,0.8": {
          v: 16,
          s: 45,
        },
        "0.93,0.72": {
          v: 17,
          s: 45,
        },
        "0.95,0.63": {
          v: 18,
          s: 45,
        },
        "0.97,0.53": {
          v: 19,
          s: 45,
        },
        "0.98,0.35": {
          v: 20,
          s: 45,
        },
        0.35: {
          v: 21,
          s: 45,
        },
        0.53: {
          v: 22,
          s: 45,
        },
        0.63: {
          v: 23,
          s: 45,
        },
        0.72: {
          v: 24,
          s: 45,
        },
        0.8: {
          v: 25,
          s: 45,
        },
        0.1: {
          v: 26,
          s: 45,
        },
        0.2: {
          v: 27,
          s: 45,
        },
        0.3: {
          v: 28,
          s: 45,
        },
        0.4: {
          v: 29,
          s: 45,
        },
        0.5: {
          v: 30,
          s: 45,
        },
        0.6000000000000001: {
          v: 31,
          s: 45,
        },
        0.7000000000000001: {
          v: 32,
          s: 45,
        },
        "Confidence Threshold (Count distinct)": {
          v: 33,
          s: 45,
        },
        "Accuracy (Sum)": {
          v: 34,
          s: 50,
        },
        "Coverage (Sum)": {
          v: 35,
          s: 50,
        },
        "6.4_teste_power:sim": {
          v: 36,
          s: 51,
        },
        "Aplicar Narrowband": {
          v: 37,
          s: 51,
        },
        "CPF Nao reconhecido": {
          v: 38,
          s: 51,
        },
        "Conexão:não": {
          v: 39,
          s: 51,
        },
        "Consumo Voz": {
          v: 40,
          s: 51,
        },
        "Descrever planos disponíveis": {
          v: 41,
          s: 51,
        },
        "Em outros casos": {
          v: 42,
          s: 51,
        },
        "Equipamento utilizado pelo cliente é um computador ou notebook:não": {
          v: 43,
          s: 51,
        },
        "Migrar Plano": {
          v: 44,
          s: 51,
        },
        "Nome da mãe reconhecido": {
          v: 45,
          s: 51,
        },
        "Reconhecer CPF": {
          v: 46,
          s: 51,
        },
        "Solicitar Cancelamento": {
          v: 47,
          s: 51,
        },
        "Valida sintoma:sim": {
          v: 0,
          s: 51,
        },
        "Validação_PL:sim": {
          v: 1,
          s: 51,
        },
        "assistant Suggestion": {
          v: 2,
          s: 51,
        },
        node_5_1621014429151: {
          v: 3,
          s: 51,
        },
        node_7_1625663634284: {
          v: 4,
          s: 51,
        },
        "(no value),6.4_teste_power:sim": {
          v: 5,
          s: 46,
        },
        "(no value),Aplicar Narrowband": {
          v: 6,
          s: 46,
        },
        "(no value),CPF Nao reconhecido": {
          v: 7,
          s: 46,
        },
        "(no value),Conexão:não": {
          v: 8,
          s: 46,
        },
        "(no value),Em outros casos": {
          v: 9,
          s: 46,
        },
        "(no value),Equipamento utilizado pelo cliente é um computador ou notebook:não":
          {
            v: 10,
            s: 46,
          },
        "(no value),Nome da mãe reconhecido": {
          v: 11,
          s: 46,
        },
        "(no value),Reconhecer CPF": {
          v: 12,
          s: 46,
        },
        "(no value),node_5_1621014429151": {
          v: 13,
          s: 46,
        },
        "Falha_de_Acesso,Validação_PL:sim": {
          v: 14,
          s: 46,
        },
        "General_Ending,Em outros casos": {
          v: 15,
          s: 46,
        },
        "General_Ending,Valida sintoma:sim": {
          v: 16,
          s: 46,
        },
        "General_Ending,node_7_1625663634284": {
          v: 17,
          s: 46,
        },
        "General_Greetings,Conexão:não": {
          v: 18,
          s: 46,
        },
        "General_Jokes,Em outros casos": {
          v: 19,
          s: 46,
        },
        "cancelar_linha,Solicitar Cancelamento": {
          v: 20,
          s: 46,
        },
        "consumo_internet,assistant Suggestion": {
          v: 21,
          s: 46,
        },
        "consumo_voz,Consumo Voz": {
          v: 22,
          s: 46,
        },
        "descrever_planos,Descrever planos disponíveis": {
          v: 23,
          s: 46,
        },
        "migrar_plano,Migrar Plano": {
          v: 24,
          s: 46,
        },
      },
      saveId: 51,
    },
    pageContext: [],
    dataSources: {
      version: "1.0",
      sources: [
        {
          id: "model0000017e7cdcde38_00000002",
          assetId: "assetId0000017e7cdcde38_00000000",
          clientId: "Db2Table1",
          module: {
            xsd: xsd,
            source: {
              id: "StringID",
              jdbc: {
                jdbcUrl: jdbcUrl,
                driverClassName: driverClassName,
                connectionProperties: "OPTIONAL=TRUE;LOGINTIMEOUT=0",
                schema: schema,
              },
              user: user,
              password: password,
            },
            table: {
              name: `${logsTable}`,
              description: "First Table",
              column: [
                {
                  name: "IDUSER",
                  description: "String",
                  datatype: "VARCHAR",
                  nullable: false,
                  label: "ID User",
                  usage: "identifier",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "CONVERSATIONID",
                  description: "String",
                  datatype: "VARCHAR",
                  nullable: false,
                  label: "ID Conversation",
                  usage: "attribute",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "LOGID",
                  description: "String",
                  datatype: "VARCHAR",
                  nullable: false,
                  label: "ID Log",
                  usage: "attribute",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "CLIENTMESSAGE",
                  description: "String",
                  datatype: "CLOB",
                  nullable: true,
                  label: "Client Message",
                  usage: "attribute",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "CLIENTTIMESTAMP",
                  description: "String",
                  datatype: "TIMESTAMP",
                  nullable: true,
                  label: "Client Timestamp",
                  usage: "attribute",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "ASSISTANTMESSAGE",
                  description: "String",
                  datatype: "CLOB",
                  nullable: true,
                  label: "Assistant Message",
                  usage: "attribute",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "ASSISTANTTIMESTAMP",
                  description: "String",
                  datatype: "TIMESTAMP",
                  nullable: true,
                  label: "Assistant Timestamp",
                  usage: "attribute",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "NODETITLE",
                  description: "String",
                  datatype: "VARCHAR",
                  nullable: true,
                  label: "Node Title",
                  usage: "attribute",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "SENTIMENT",
                  description: "String",
                  datatype: "REAL",
                  nullable: true,
                  label: "Client Message Sentiment",
                  usage: "attribute",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "FIRSTINTENT",
                  description: "String",
                  datatype: "VARCHAR",
                  nullable: true,
                  label: "First Intent",
                  usage: "attribute",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "FIRSTINTENTCONFIDENCE",
                  description: "String",
                  datatype: "REAL",
                  nullable: true,
                  label: "First Intent Confidence",
                  usage: "attribute",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "INTENTS",
                  description: "String",
                  datatype: "VARCHAR",
                  nullable: true,
                  label: "First Intent Confidence",
                  usage: "attribute",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "INTENTSCONFIDENCE",
                  description: "String",
                  datatype: "VARCHAR",
                  nullable: true,
                  label: "All Intents",
                  usage: "attribute",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "ENTITIES",
                  description: "String",
                  datatype: "VARCHAR",
                  nullable: true,
                  label: "All Confidences",
                  usage: "attribute",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "ERROR",
                  description: "String",
                  datatype: "VARCHAR",
                  nullable: true,
                  label: "Error",
                  usage: "attribute",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "SCORE",
                  description: "String",
                  datatype: "INTEGER",
                  nullable: true,
                  label: "Curator Score",
                  usage: "attribute",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
              ],
            },
            label: "Db2 Module",
            identifier: "db2module1Table",
          },
          name: "Logs Table",
          shaping: {
            shapingId: "shaping0000017ec005411d_00000000",
            embeddedModuleUpToDate: false,
            moserJSON: {
              version: "11.0",
              container: "C",
              useSpec: [
                {
                  identifier: "ES",
                  type: "url",
                  storeID: "baseModule",
                  imports: "*",
                },
              ],
              expressionLocale: "en-us",
              querySubject: [
                {
                  ref: [`ES.${logsTable}`],
                  instanceType: "reference",
                  item: [
                    {
                      queryItem: {
                        expression: "SCORE",
                        usage: "fact",
                        datatype: "INTEGER",
                        nullable: true,
                        regularAggregate: "average",
                        facetDefinition: {
                          enabled: "automatic",
                        },
                        identifier: "C_Curator_Score",
                        label: "C_Curator Score",
                        property: [
                          {
                            name: "_MUI_expr",
                            value:
                              '{"func":"customCalculation","version":"11.0"}',
                          },
                          {
                            name: "_MUI_propertySetByUser_regularAggregate",
                            value: '"true"',
                          },
                          {
                            name: "_MUI_propertySetByUser_usage",
                            value: '"true"',
                          },
                          {
                            name: "_path",
                            value: "0",
                          },
                        ],
                        propertyOverride: ["NEW"],
                      },
                    },
                    {
                      queryItem: {
                        expression:
                          "count(LOGID)/count (distinct CONVERSATIONID)",
                        usage: "fact",
                        datatype: "DOUBLE",
                        nullable: true,
                        regularAggregate: "calculated",
                        facetDefinition: {
                          enabled: "false",
                        },
                        identifier: "Média_mensagem_conversa",
                        label: "Média mensagem/conversa",
                        property: [
                          {
                            name: "_MUI_expr",
                            value:
                              '{"func":"customCalculation","version":"11.0"}',
                          },
                          {
                            name: "_MUI_propertySetByUser_regularAggregate",
                            value: '"true"',
                          },
                          {
                            name: "_path",
                            value: "0",
                          },
                        ],
                        propertyOverride: ["NEW"],
                      },
                    },
                    {
                      queryItem: {
                        usage: "identifier",
                        regularAggregate: "countDistinct",
                        identifier: "CONVERSATIONID",
                        property: [
                          {
                            name: "_MUI_propertySetByUser_usage",
                            value: '"true"',
                          },
                          {
                            name: "_MUI_propertySetByUser_regularAggregate",
                            value: '"true"',
                          },
                          {
                            name: "_path",
                            value: "2",
                          },
                        ],
                        propertyOverride: [
                          "usage",
                          "regularAggregate",
                          "property._MUI_propertySetByUser_usage",
                          "property._MUI_propertySetByUser_regularAggregate",
                        ],
                      },
                    },
                    {
                      queryItem: {
                        usage: "identifier",
                        regularAggregate: "countDistinct",
                        identifier: "LOGID",
                        property: [
                          {
                            name: "_MUI_propertySetByUser_usage",
                            value: '"true"',
                          },
                          {
                            name: "_MUI_propertySetByUser_regularAggregate",
                            value: '"true"',
                          },
                          {
                            name: "_path",
                            value: "3",
                          },
                        ],
                        propertyOverride: [
                          "usage",
                          "regularAggregate",
                          "property._MUI_propertySetByUser_usage",
                          "property._MUI_propertySetByUser_regularAggregate",
                        ],
                      },
                    },
                  ],
                  identifier: `${logsTable}`,
                  idForExpression: `${logsTable}`,
                },
              ],
              dataRetrievalMode: "liveConnection",
              identifier: "C_newModel",
              label: "newModel",
            },
          },
        },
        {
          id: "model0000017e7cdcde39_00000002",
          assetId: "assetId0000017e7cdcde39_00000000",
          clientId: "Db2Table2",
          module: {
            xsd: xsd,
            source: {
              id: "StringID",
              jdbc: {
                jdbcUrl: jdbcUrl,
                driverClassName: driverClassName,
                connectionProperties: "OPTIONAL=TRUE;LOGINTIMEOUT=0",
                schema: schema,
              },
              user: user,
              password: password,
            },
            table: {
              name: `${conversationTable}`,
              description: "Second Table",
              column: [
                {
                  name: "IDUSER",
                  description: "String",
                  datatype: "VARCHAR",
                  nullable: false,
                  label: "ID User",
                  usage: "identifier",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "CONVERSATIONID",
                  description: "String",
                  datatype: "VARCHAR",
                  nullable: false,
                  label: "ID Conversation",
                  usage: "attribute",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "CHANNEL",
                  description: "String",
                  datatype: "VARCHAR",
                  nullable: true,
                  label: "Conversation Channel",
                  usage: "attribute",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "STARTTIME",
                  description: "String",
                  datatype: "TIMESTAMP",
                  nullable: true,
                  label: "Conversation Start Time",
                  usage: "attribute",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "TIMEINTERVAL",
                  description: "String",
                  datatype: "INTEGER",
                  nullable: false,
                  label: "Conversation Duration",
                  usage: "attribute",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "FEEDBACK",
                  description: "String",
                  datatype: "SMALLINT",
                  nullable: true,
                  label: "Conversation Feedback",
                  usage: "attribute",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "TRANSFERED",
                  description: "String",
                  datatype: "BOOLEAN",
                  nullable: true,
                  label: "User Transfered?",
                  usage: "attribute",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "RELEVANCE",
                  description: "String",
                  datatype: "BOOLEAN",
                  nullable: true,
                  label: "Conversation Relevant?",
                  usage: "attribute",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "NEWUSER",
                  description: "String",
                  datatype: "BOOLEAN",
                  nullable: false,
                  label: "New User?",
                  usage: "attribute",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
              ],
            },
            label: "Db2 Module",
            identifier: "db2module2Table",
          },
          name: "Conversation Table",
          shaping: {
            shapingId: "shaping0000017ea16dbf14_00000000",
            embeddedModuleUpToDate: false,
            moserJSON: {
              version: "11.0",
              container: "C",
              useSpec: [
                {
                  identifier: "ES",
                  type: "url",
                  storeID: "baseModule",
                  imports: "*",
                },
              ],
              expressionLocale: "en-us",
              querySubject: [
                {
                  ref: [`ES.${conversationTable}`],
                  instanceType: "reference",
                  item: [
                    {
                      queryItem: {
                        expression: "quartile(TIMEINTERVAL)",
                        usage: "fact",
                        datatype: "long",
                        regularAggregate: "total",
                        facetDefinition: {
                          enabled: "automatic",
                        },
                        identifier: "durationQuartile",
                        label: "durationQuartile",
                        property: [
                          {
                            name: "_MUI_expr",
                            value:
                              '{"func":"customCalculation","version":"11.0"}',
                          },
                          {
                            name: "_MUI_propertySetByUser_usage",
                            value: '"true"',
                          },
                          {
                            name: "_path",
                            value: "0",
                          },
                        ],
                        propertyOverride: ["NEW"],
                      },
                    },
                    {
                      queryItem: {
                        expression: "( FEEDBACK = 0)",
                        usage: "fact",
                        datatype: "long",
                        regularAggregate: "total",
                        facetDefinition: {
                          enabled: "false",
                        },
                        identifier: "noFeedback",
                        label: "No Feedback",
                        property: [
                          {
                            name: "_MUI_expr",
                            value:
                              '{"func":"customCalculation","version":"11.0"}',
                          },
                          {
                            name: "_path",
                            value: "1",
                          },
                        ],
                        propertyOverride: ["NEW"],
                      },
                    },
                    {
                      queryItem: {
                        expression: "(FEEDBACK = 10)",
                        usage: "fact",
                        datatype: "long",
                        regularAggregate: "total",
                        facetDefinition: {
                          enabled: "false",
                        },
                        identifier: "perfectFeedback",
                        label: "10",
                        property: [
                          {
                            name: "_MUI_expr",
                            value:
                              '{"func":"customCalculation","version":"11.0"}',
                          },
                          {
                            name: "_path",
                            value: "2",
                          },
                        ],
                        propertyOverride: ["NEW"],
                      },
                    },
                    {
                      queryItem: {
                        expression: "(FEEDBACK between 7 and 9)",
                        usage: "fact",
                        datatype: "long",
                        regularAggregate: "total",
                        facetDefinition: {
                          enabled: "false",
                        },
                        identifier: "goodFeedback",
                        label: "7 - 9",
                        property: [
                          {
                            name: "_MUI_expr",
                            value:
                              '{"func":"customCalculation","version":"11.0"}',
                          },
                          {
                            name: "_path",
                            value: "3",
                          },
                        ],
                        propertyOverride: ["NEW"],
                      },
                    },
                    {
                      queryItem: {
                        expression: "(FEEDBACK between 4 and 6)",
                        usage: "fact",
                        datatype: "long",
                        regularAggregate: "total",
                        facetDefinition: {
                          enabled: "false",
                        },
                        identifier: "mediumFeedback",
                        label: "4 - 6",
                        property: [
                          {
                            name: "_MUI_expr",
                            value:
                              '{"func":"customCalculation","version":"11.0"}',
                          },
                          {
                            name: "_path",
                            value: "4",
                          },
                        ],
                        propertyOverride: ["NEW"],
                      },
                    },
                    {
                      queryItem: {
                        expression: "(FEEDBACK between 1 and 3)",
                        usage: "fact",
                        datatype: "long",
                        regularAggregate: "total",
                        facetDefinition: {
                          enabled: "false",
                        },
                        identifier: "C_Conversation_Feedbac",
                        label: "1 - 3 ",
                        property: [
                          {
                            name: "_MUI_expr",
                            value:
                              '{"func":"customCalculation","version":"11.0"}',
                          },
                          {
                            name: "_path",
                            value: "5",
                          },
                        ],
                        propertyOverride: ["NEW"],
                      },
                    },
                    {
                      queryItem: {
                        expression: "(FEEDBACK<>0)",
                        usage: "fact",
                        datatype: "long",
                        regularAggregate: "total",
                        facetDefinition: {
                          enabled: "false",
                        },
                        identifier: "Calculation_name",
                        label: "withFeedback",
                        property: [
                          {
                            name: "_MUI_expr",
                            value:
                              '{"func":"customCalculation","version":"11.0"}',
                          },
                          {
                            name: "_path",
                            value: "6",
                          },
                        ],
                        propertyOverride: ["NEW"],
                      },
                    },
                    {
                      queryItem: {
                        expression: "count(nullif(NEWUSER,1))",
                        usage: "fact",
                        datatype: "LONG",
                        nullable: true,
                        regularAggregate: "calculated",
                        facetDefinition: {
                          enabled: "false",
                        },
                        identifier: "reccurrentUsers",
                        label: "Recurrent Users",
                        property: [
                          {
                            name: "_MUI_expr",
                            value:
                              '{"func":"customCalculation","version":"11.0"}',
                          },
                          {
                            name: "_MUI_propertySetByUser_regularAggregate",
                            value: '"true"',
                          },
                          {
                            name: "_path",
                            value: "7",
                          },
                        ],
                        propertyOverride: ["NEW"],
                      },
                    },
                    {
                      queryItem: {
                        expression: "count(nullif(NEWUSER,0))",
                        usage: "fact",
                        datatype: "LONG",
                        nullable: true,
                        regularAggregate: "calculated",
                        facetDefinition: {
                          enabled: "false",
                        },
                        identifier: "newUser_1",
                        label: "New Users",
                        property: [
                          {
                            name: "_MUI_expr",
                            value:
                              '{"func":"customCalculation","version":"11.0"}',
                          },
                          {
                            name: "_MUI_propertySetByUser_regularAggregate",
                            value: '"true"',
                          },
                          {
                            name: "_path",
                            value: "8",
                          },
                        ],
                        propertyOverride: ["NEW"],
                      },
                    },
                    {
                      queryItem: {
                        expression:
                          "relevantConversations*100/count( distinct CONVERSATIONID)",
                        usage: "fact",
                        datatype: "DOUBLE",
                        nullable: true,
                        regularAggregate: "total",
                        facetDefinition: {
                          enabled: "false",
                        },
                        identifier: "i_relevant",
                        label: "%relevant",
                        property: [
                          {
                            name: "_MUI_expr",
                            value:
                              '{"func":"customCalculation","version":"11.0"}',
                          },
                          {
                            name: "_path",
                            value: "9",
                          },
                        ],
                        propertyOverride: ["NEW"],
                      },
                    },
                    {
                      queryItem: {
                        expression: "count(nullif(RELEVANCE,0))",
                        usage: "fact",
                        datatype: "LONG",
                        nullable: true,
                        regularAggregate: "calculated",
                        facetDefinition: {
                          enabled: "false",
                        },
                        identifier: "relevantConversations",
                        label: "relevantConversations",
                        property: [
                          {
                            name: "_MUI_expr",
                            value:
                              '{"func":"customCalculation","version":"11.0"}',
                          },
                          {
                            name: "_MUI_propertySetByUser_usage",
                            value: '"true"',
                          },
                          {
                            name: "_MUI_propertySetByUser_regularAggregate",
                            value: '"true"',
                          },
                          {
                            name: "_path",
                            value: "10",
                          },
                        ],
                        propertyOverride: ["NEW"],
                      },
                    },
                    {
                      queryItem: {
                        expression:
                          "C_User_Transfered*100/count( distinct CONVERSATIONID)",
                        usage: "fact",
                        datatype: "DOUBLE",
                        nullable: true,
                        regularAggregate: "calculated",
                        facetDefinition: {
                          enabled: "false",
                        },
                        identifier: "C_transferedUsers",
                        label: "%transfered",
                        property: [
                          {
                            name: "_MUI_expr",
                            value:
                              '{"func":"customCalculation","version":"11.0"}',
                          },
                          {
                            name: "_path",
                            value: "11",
                          },
                        ],
                        propertyOverride: ["NEW"],
                      },
                    },
                    {
                      queryItem: {
                        expression: "count(nullif(TRANSFERED,0))",
                        usage: "fact",
                        datatype: "LONG",
                        nullable: true,
                        regularAggregate: "calculated",
                        facetDefinition: {
                          enabled: "false",
                        },
                        identifier: "C_User_Transfered",
                        label: "transferedConversations",
                        property: [
                          {
                            name: "_MUI_expr",
                            value:
                              '{"func":"customCalculation","version":"11.0"}',
                          },
                          {
                            name: "_MUI_propertySetByUser_usage",
                            value: '"true"',
                          },
                          {
                            name: "_MUI_propertySetByUser_regularAggregate",
                            value: '"true"',
                          },
                          {
                            name: "_path",
                            value: "12",
                          },
                        ],
                        propertyOverride: ["NEW"],
                      },
                    },
                    {
                      queryItem: {
                        expression:
                          "if ( CHANNEL = 'Chat') then (running-count(CHANNEL)) else NULL",
                        usage: "fact",
                        datatype: "LONG",
                        nullable: true,
                        regularAggregate: "countDistinct",
                        facetDefinition: {
                          enabled: "false",
                        },
                        identifier: "Chat_Count",
                        label: "Chat Count",
                        property: [
                          {
                            name: "_MUI_expr",
                            value:
                              '{"func":"customCalculation","version":"11.0"}',
                          },
                          {
                            name: "_MUI_propertySetByUser_regularAggregate",
                            value: '"true"',
                          },
                          {
                            name: "_path",
                            value: "13",
                          },
                        ],
                        propertyOverride: ["NEW"],
                      },
                    },
                    {
                      queryItem: {
                        expression:
                          "if ( CHANNEL = 'Phone') then (running-count(CHANNEL)) else NULL",
                        usage: "fact",
                        datatype: "LONG",
                        nullable: true,
                        regularAggregate: "countDistinct",
                        facetDefinition: {
                          enabled: "false",
                        },
                        identifier: "Phone_Count",
                        label: "Phone Count",
                        property: [
                          {
                            name: "_MUI_expr",
                            value:
                              '{"func":"customCalculation","version":"11.0"}',
                          },
                          {
                            name: "_MUI_propertySetByUser_usage",
                            value: '"true"',
                          },
                          {
                            name: "_MUI_propertySetByUser_regularAggregate",
                            value: '"true"',
                          },
                          {
                            name: "_path",
                            value: "14",
                          },
                        ],
                        propertyOverride: ["NEW"],
                      },
                    },
                    {
                      queryItem: {
                        expression:
                          "_make_timestamp(_year(STARTTIME),_month(STARTTIME),_day(STARTTIME))",
                        usage: "attribute",
                        format:
                          '{"formatGroup":{"dateFormat":{"dateSeparator":"/","dateStyle":"short","displayOrder":"DMY"}}}',
                        datatype: "TIMESTAMP",
                        nullable: true,
                        regularAggregate: "minimum",
                        facetDefinition: {
                          enabled: "automatic",
                        },
                        identifier: "C_Conversation_Start_T",
                        label: "simpleDate",
                        property: [
                          {
                            name: "_MUI_expr",
                            value:
                              '{"func":"customCalculation","version":"11.0"}',
                          },
                          {
                            name: "_MUI_format",
                            value: '{"advancedMode":false}',
                          },
                          {
                            name: "_path",
                            value: "15",
                          },
                        ],
                        propertyOverride: ["NEW"],
                      },
                    },
                    {
                      queryItem: {
                        usage: "identifier",
                        regularAggregate: "count",
                        identifier: "CONVERSATIONID",
                        property: [
                          {
                            name: "_MUI_propertySetByUser_usage",
                            value: '"true"',
                          },
                          {
                            name: "_path",
                            value: "17",
                          },
                        ],
                        propertyOverride: [
                          "property._MUI_propertySetByUser_usage",
                          "usage",
                          "regularAggregate",
                        ],
                      },
                    },
                    {
                      queryItem: {
                        usage: "attribute",
                        regularAggregate: "count",
                        identifier: "CHANNEL",
                        property: [
                          {
                            name: "_MUI_propertySetByUser_usage",
                            value: '"true"',
                          },
                          {
                            name: "_MUI_propertySetByUser_regularAggregate",
                            value: '"true"',
                          },
                          {
                            name: "_path",
                            value: "18",
                          },
                        ],
                        propertyOverride: [
                          "usage",
                          "regularAggregate",
                          "property._MUI_propertySetByUser_usage",
                          "property._MUI_propertySetByUser_regularAggregate",
                        ],
                      },
                    },
                    {
                      queryItem: {
                        usage: "identifier",
                        format:
                          '{"formatGroup":{"dateFormat":{"dateSeparator":"/","dateStyle":"short","displayOrder":"DMY"}}}',
                        regularAggregate: "count",
                        facetDefinition: {
                          enabled: "false",
                          sortList: {
                            sortItem: [
                              {
                                ref: "CONVERSATIONID",
                                order: "ascending",
                                nullPlacement: "first",
                              },
                            ],
                          },
                        },
                        taxonomy: [
                          {
                            domain: "cognos",
                            class: "cNone",
                            family: "cNone",
                          },
                        ],
                        identifier: "STARTTIME",
                        property: [
                          {
                            name: "_MUI_propertySetByUser_taxonomy",
                            value: '"true"',
                          },
                          {
                            name: "_MUI_propertySetByUser_facetDefinition",
                            value: '"true"',
                          },
                          {
                            name: "_MUI_format",
                            value: '{"advancedMode":false}',
                          },
                          {
                            name: "_MUI_propertySetByUser_regularAggregate",
                            value: '"true"',
                          },
                          {
                            name: "_MUI_propertySetByUser_usage",
                            value: '"true"',
                          },
                          {
                            name: "_path",
                            value: "19",
                          },
                        ],
                        propertyOverride: [
                          "usage",
                          "regularAggregate",
                          "facetDefinition",
                          "format",
                          "property._MUI_propertySetByUser_taxonomy",
                          "property._MUI_propertySetByUser_facetDefinition",
                          "property._MUI_format",
                          "property._MUI_propertySetByUser_regularAggregate",
                          "property._MUI_propertySetByUser_usage",
                          "taxonomy",
                        ],
                      },
                    },
                    {
                      queryItem: {
                        usage: "fact",
                        format: '{"formatGroup":{"numberFormat":{}}}',
                        regularAggregate: "total",
                        identifier: "TIMEINTERVAL",
                        property: [
                          {
                            name: "_MUI_propertySetByUser_usage",
                            value: '"true"',
                          },
                          {
                            name: "_MUI_format",
                            value: '{"advancedMode":false}',
                          },
                          {
                            name: "_path",
                            value: "20",
                          },
                        ],
                        propertyOverride: [
                          "property._MUI_propertySetByUser_usage",
                          "usage",
                          "regularAggregate",
                          "format",
                          "property._MUI_format",
                        ],
                      },
                    },
                    {
                      queryItem: {
                        usage: "attribute",
                        regularAggregate: "countDistinct",
                        identifier: "TRANSFERED",
                        property: [
                          {
                            name: "_MUI_propertySetByUser_usage",
                            value: '"true"',
                          },
                          {
                            name: "_MUI_propertySetByUser_regularAggregate",
                            value: '"true"',
                          },
                          {
                            name: "_path",
                            value: "22",
                          },
                        ],
                        propertyOverride: [
                          "usage",
                          "regularAggregate",
                          "property._MUI_propertySetByUser_usage",
                          "property._MUI_propertySetByUser_regularAggregate",
                        ],
                      },
                    },
                  ],
                  identifier: `${conversationTable}`,
                  idForExpression: `${conversationTable}`,
                },
              ],
              dataRetrievalMode: "liveConnection",
              identifier: "C_newModel",
              label: "newModel",
            },
          },
        },
        {
          id: "model0000017e7cdcde3a_00000000",
          assetId: "assetId0000017e7cdcde39_00000003",
          clientId: "Db2Table3",
          module: {
            xsd: xsd,
            source: {
              id: "StringID",
              jdbc: {
                jdbcUrl: jdbcUrl,
                driverClassName: driverClassName,
                connectionProperties: "OPTIONAL=TRUE;LOGINTIMEOUT=0",
                schema: schema,
              },
              user: user,
              password: password,
            },
            table: {
              name: `${callsTable}`,
              description: "Third Table",
              column: [
                {
                  name: "IDUSER",
                  description: "String",
                  datatype: "VARCHAR",
                  nullable: false,
                  label: "ID User",
                  usage: "identifier",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "CONVERSATIONID",
                  description: "String",
                  datatype: "VARCHAR",
                  nullable: false,
                  label: "ID Conversation",
                  usage: "attribute",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "USERNUMBER",
                  description: "String",
                  datatype: "VARCHAR",
                  nullable: true,
                  label: "User Phone Number",
                  usage: "attribute",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "USERIPADDRESS",
                  description: "String",
                  datatype: "VARCHAR",
                  nullable: true,
                  label: "User IP Adress",
                  usage: "attribute",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "VGWISDTMF",
                  description: "String",
                  datatype: "BOOLEAN",
                  nullable: true,
                  label: "VGW is DTMF",
                  usage: "attribute",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "VGWBARGEINOCCURRED",
                  description: "String",
                  datatype: "BOOLEAN",
                  nullable: true,
                  label: "Barge in Occurred",
                  usage: "attribute",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "VGWPHONEUSERPHONENUMBER",
                  description: "String",
                  datatype: "VARCHAR",
                  nullable: true,
                  label: "VGW User Phone Number",
                  usage: "attribute",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "VGWDTMFCOLLECTIONSUCCEEDED",
                  description: "String",
                  datatype: "BOOLEAN",
                  nullable: true,
                  label: "VGW Collection Succeeded",
                  usage: "attribute",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "CONCLUDED",
                  description: "String",
                  datatype: "BOOLEAN",
                  nullable: true,
                  label: "Finished Conversations",
                  usage: "attribute",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
              ],
            },
            label: "Db2 Module",
            identifier: "db2module3Table",
          },
          name: "Calls Table",
          shaping: {
            shapingId: "shaping0000018060d3a699_00000000",
            embeddedModuleUpToDate: false,
            moserJSON: {
              version: "11.0",
              container: "C",
              useSpec: [
                {
                  identifier: "ES",
                  type: "url",
                  storeID: "baseModule",
                  imports: "*",
                },
              ],
              expressionLocale: "en-us",
              querySubject: [
                {
                  ref: [`ES.${callsTable}`],
                  instanceType: "reference",
                  item: [
                    {
                      queryItem: {
                        expression: "count(nullif(CONCLUDED, 1))",
                        usage: "fact",
                        datatype: "LONG",
                        nullable: true,
                        regularAggregate: "calculated",
                        facetDefinition: {
                          enabled: "automatic",
                        },
                        identifier: "unfinishedConversations",
                        label: "unfinishedConversations",
                        property: [
                          {
                            name: "_MUI_expr",
                            value:
                              '{"func":"customCalculation","version":"11.0"}',
                          },
                          {
                            name: "_MUI_propertySetByUser_usage",
                            value: '"true"',
                          },
                          {
                            name: "_MUI_propertySetByUser_regularAggregate",
                            value: '"true"',
                          },
                          {
                            name: "_path",
                            value: "0",
                          },
                        ],
                        propertyOverride: ["NEW"],
                      },
                    },
                    {
                      queryItem: {
                        expression:
                          "C_Finished_Conversatio*100/count( distinct CONVERSATIONID )",
                        usage: "fact",
                        datatype: "DOUBLE",
                        nullable: true,
                        regularAggregate: "calculated",
                        facetDefinition: {
                          enabled: "false",
                        },
                        identifier: "C_concludedConversatio",
                        label: "%concluded",
                        property: [
                          {
                            name: "_MUI_expr",
                            value:
                              '{"func":"customCalculation","version":"11.0"}',
                          },
                          {
                            name: "_MUI_propertySetByUser_regularAggregate",
                            value: '"true"',
                          },
                          {
                            name: "_path",
                            value: "1",
                          },
                        ],
                        propertyOverride: ["NEW"],
                      },
                    },
                    {
                      queryItem: {
                        expression: "count(nullif(CONCLUDED,0))",
                        usage: "fact",
                        datatype: "LONG",
                        nullable: true,
                        regularAggregate: "calculated",
                        facetDefinition: {
                          enabled: "false",
                        },
                        identifier: "C_Finished_Conversatio",
                        label: "concludedConversations",
                        property: [
                          {
                            name: "_MUI_expr",
                            value:
                              '{"func":"customCalculation","version":"11.0"}',
                          },
                          {
                            name: "_MUI_propertySetByUser_usage",
                            value: '"true"',
                          },
                          {
                            name: "_MUI_propertySetByUser_regularAggregate",
                            value: '"true"',
                          },
                          {
                            name: "_path",
                            value: "2",
                          },
                        ],
                        propertyOverride: ["NEW"],
                      },
                    },
                  ],
                  identifier: `${callsTable}`,
                  idForExpression: `${callsTable}`,
                },
              ],
              dataRetrievalMode: "liveConnection",
              identifier: "C_newModel",
              label: "newModel",
            },
          },
        },
        {
          id: "model0000017e7cdcde3a_00000003",
          assetId: "assetId0000017e7cdcde3a_00000001",
          clientId: "Db2Table4",
          module: {
            xsd: xsd,
            source: {
              id: "StringID",
              jdbc: {
                jdbcUrl: jdbcUrl,
                driverClassName: driverClassName,
                connectionProperties: "OPTIONAL=TRUE;LOGINTIMEOUT=0",
                schema: schema,
              },
              user: user,
              password: password,
            },
            table: {
              name: `${contextTable}`,
              description: "Fourth Table",
              column: [
                {
                  name: "CONVERSATIONID",
                  description: "String",
                  datatype: "VARCHAR",
                  nullable: false,
                  label: "ID Conversation",
                  usage: "identifier",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "ENVVARIABLENAME",
                  description: "String",
                  datatype: "VARCHAR",
                  nullable: false,
                  label: "Identified Enviroment Variable",
                  usage: "attribute",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "ENVVARIABLEVALUE",
                  description: "String",
                  datatype: "VARCHAR",
                  nullable: true,
                  label: "Enviroment Variable Value",
                  usage: "attribute",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "ENVVARIABLETYPE",
                  description: "String",
                  datatype: "VARCHAR",
                  nullable: true,
                  label: "Enviroment Variable Type",
                  usage: "attribute",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
              ],
            },
            label: "Db2 Module",
            identifier: "db2module4Table",
          },
          name: "Context Table",
          shaping: {
            embeddedModuleUpToDate: true,
          },
        },
        {
          id: "model0000017e7cdcde3a_00000013",
          assetId: "assetId0000017e7cdcde3a_00000001",
          clientId: "Db2Table4",
          module: {
            xsd: xsd,
            source: {
              id: "StringID",
              jdbc: {
                jdbcUrl: jdbcUrl,
                driverClassName: driverClassName,
                connectionProperties: "OPTIONAL=TRUE;LOGINTIMEOUT=0",
                schema: schema,
              },
              user: user,
              password: password,
            },
            table: {
              name: `${conversationPathTable}`,
              description: "Conversation Path",
              column: [
                {
                  name: "CONVERSATIONID",
                  description: "String",
                  datatype: "VARCHAR",
                  nullable: false,
                  label: "Conversation ID",
                  usage: "identifier",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "ORIGINNODE",
                  description: "String",
                  datatype: "VARCHAR",
                  nullable: false,
                  label: "Origin Node",
                  usage: "attribute",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "DESTINENODE",
                  description: "String",
                  datatype: "VARCHAR",
                  nullable: true,
                  label: "Destine Node",
                  usage: "attribute",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
              ],
            },
            label: "Db2 Module",
            identifier: "db2module4Table",
          },
          name: "Conversation Path",
          shaping: {
            embeddedModuleUpToDate: true,
          },
        },
        {
          id: "model0000017fff1217ca_00000002",
          assetId: "assetId0000017fff1217ca_00000000",
          clientId: "fifthTable",
          module: {
            xsd: xsd,
            source: {
              id: "StringID",
              jdbc: {
                jdbcUrl: jdbcUrl,
                driverClassName: driverClassName,
                connectionProperties: "OPTIONAL=TRUE;LOGINTIMEOUT=0",
                schema: schema,
              },
              user: user,
              password: password,
            },
            table: {
              name: `${overviewTable}`,
              description: "Overview",
              column: [
                {
                  name: "METRIC",
                  description: "String",
                  datatype: "VARCHAR",
                  nullable: false,
                  label: "Metric",
                  usage: "identifier",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "VALUE",
                  description: "String",
                  datatype: "DECIMAL",
                  nullable: false,
                  label: "Value",
                  usage: "attribute",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
              ],
            },
            label: "Db2 Module",
            identifier: "db2module1Table",
          },
          name: "Overview",
          shaping: {
            embeddedModuleUpToDate: true,
          },
        },
        {
          id: "model0000017fff1217cc_00000002",
          assetId: "assetId0000017fff1217cc_00000000",
          clientId: "6Table",
          module: {
            xsd: xsd,
            source: {
              id: "StringID",
              jdbc: {
                jdbcUrl: jdbcUrl,
                driverClassName: driverClassName,
                connectionProperties: "OPTIONAL=TRUE;LOGINTIMEOUT=0",
                schema: schema,
              },
              user: user,
              password: password,
            },
            table: {
              name: `${classDistributionTable}`,
              description: "ClassDistribution",
              column: [
                {
                  name: "INTENT",
                  description: "String",
                  datatype: "VARCHAR",
                  nullable: false,
                  label: "Intent",
                  usage: "identifier",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "COUNT",
                  description: "String",
                  datatype: "INTEGER",
                  nullable: false,
                  label: "Count",
                  usage: "attribute",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
              ],
            },
            label: "Db2 Module",
            identifier: "db2module1Table",
          },
          name: "Class Distribution",
          shaping: {
            shapingId: "shaping0000017fff194f7d_00000000",
            embeddedModuleUpToDate: false,
            moserJSON: {
              version: "11.0",
              container: "C",
              useSpec: [
                {
                  identifier: "ES",
                  type: "url",
                  storeID: "baseModule",
                  imports: "*",
                },
              ],
              expressionLocale: "en-us",
              querySubject: [
                {
                  ref: [`ES.${classDistributionTable}`],
                  instanceType: "reference",
                  item: [
                    {
                      queryItem: {
                        usage: "fact",
                        regularAggregate: "total",
                        identifier: "COUNT_",
                        property: [
                          {
                            name: "_MUI_propertySetByUser_usage",
                            value: '"true"',
                          },
                          {
                            name: "_path",
                            value: "1",
                          },
                        ],
                        propertyOverride: [
                          "property._MUI_propertySetByUser_usage",
                          "usage",
                          "regularAggregate",
                        ],
                      },
                    },
                  ],
                  identifier: `${classDistributionTable}`,
                  idForExpression: `${classDistributionTable}`,
                },
              ],
              dataRetrievalMode: "liveConnection",
              identifier: "C_newModel",
              label: "newModel",
            },
          },
        },
        {
          id: "model0000017fff1217cc_00000005",
          assetId: "assetId0000017fff1217cc_00000003",
          clientId: "7Table",
          module: {
            xsd: xsd,
            source: {
              id: "StringID",
              jdbc: {
                jdbcUrl: jdbcUrl,
                driverClassName: driverClassName,
                connectionProperties: "OPTIONAL=TRUE;LOGINTIMEOUT=0",
                schema: schema,
              },
              user: user,
              password: password,
            },
            table: {
              name: `${precisionAtKTable}`,
              description: `${precisionAtKTable}`,
              column: [
                {
                  name: "K",
                  description: "String",
                  datatype: "INTEGER",
                  nullable: false,
                  label: "K",
                  usage: "identifier",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "PRECISION",
                  description: "String",
                  datatype: "DECIMAL",
                  nullable: false,
                  label: "Precision",
                  usage: "attribute",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
              ],
            },
            label: "Db2 Module",
            identifier: "db2module1Table",
          },
          name: "Precision at K",
          shaping: {
            shapingId: "shaping0000017fff1b271f_00000000",
            embeddedModuleUpToDate: false,
            moserJSON: {
              version: "11.0",
              container: "C",
              useSpec: [
                {
                  identifier: "ES",
                  type: "url",
                  storeID: "baseModule",
                  imports: "*",
                },
              ],
              expressionLocale: "en-us",
              querySubject: [
                {
                  ref: [`ES.${precisionAtKTable}`],
                  instanceType: "reference",
                  item: [
                    {
                      queryItem: {
                        usage: "fact",
                        regularAggregate: "total",
                        identifier: "PRECISION_",
                        property: [
                          {
                            name: "_MUI_propertySetByUser_usage",
                            value: '"true"',
                          },
                          {
                            name: "_path",
                            value: "1",
                          },
                        ],
                        propertyOverride: [
                          "property._MUI_propertySetByUser_usage",
                          "usage",
                          "regularAggregate",
                        ],
                      },
                    },
                  ],
                  identifier: `${precisionAtKTable}`,
                  idForExpression: `${precisionAtKTable}`,
                },
              ],
              dataRetrievalMode: "liveConnection",
              identifier: "C_newModel",
              label: "newModel",
            },
          },
        },
        {
          id: "model0000017fff1217cc_00000008",
          assetId: "assetId0000017fff1217cc_00000006",
          clientId: "8Table",
          module: {
            xsd: xsd,
            source: {
              id: "StringID",
              jdbc: {
                jdbcUrl: jdbcUrl,
                driverClassName: driverClassName,
                connectionProperties: "OPTIONAL=TRUE;LOGINTIMEOUT=0",
                schema: schema,
              },
              user: user,
              password: password,
            },
            table: {
              name: `${classAccuracyTable}`,
              description: "ClassDistribution",
              column: [
                {
                  name: "CLASS",
                  description: "String",
                  datatype: "VARCHAR",
                  nullable: false,
                  label: "Class",
                  usage: "identifier",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "COUNT",
                  description: "String",
                  datatype: "INTEGER",
                  nullable: false,
                  label: "Count",
                  usage: "attribute",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "PRECISION",
                  description: "String",
                  datatype: "DECIMAL",
                  nullable: false,
                  label: "Precision",
                  usage: "attribute",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "RECALL",
                  description: "String",
                  datatype: "DECIMAL",
                  nullable: false,
                  label: "Recall",
                  usage: "attribute",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "F1",
                  description: "String",
                  datatype: "DECIMAL",
                  nullable: false,
                  label: "F1",
                  usage: "attribute",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
              ],
            },
            label: "Db2 Module",
            identifier: "db2module1Table",
          },
          name: "Class Accuracy",
          shaping: {
            shapingId: "shaping0000017fff1ebcd2_00000000",
            embeddedModuleUpToDate: false,
            moserJSON: {
              version: "11.0",
              container: "C",
              useSpec: [
                {
                  identifier: "ES",
                  type: "url",
                  storeID: "baseModule",
                  imports: "*",
                },
              ],
              expressionLocale: "en-us",
              querySubject: [
                {
                  ref: [`ES.${classAccuracyTable}`],
                  instanceType: "reference",
                  item: [
                    {
                      queryItem: {
                        usage: "fact",
                        regularAggregate: "total",
                        identifier: "COUNT_",
                        property: [
                          {
                            name: "_MUI_propertySetByUser_usage",
                            value: '"true"',
                          },
                          {
                            name: "_path",
                            value: "1",
                          },
                        ],
                        propertyOverride: [
                          "property._MUI_propertySetByUser_usage",
                          "usage",
                          "regularAggregate",
                        ],
                      },
                    },
                    {
                      queryItem: {
                        usage: "fact",
                        regularAggregate: "total",
                        identifier: "PRECISION_",
                        property: [
                          {
                            name: "_MUI_propertySetByUser_usage",
                            value: '"true"',
                          },
                          {
                            name: "_path",
                            value: "2",
                          },
                        ],
                        propertyOverride: [
                          "property._MUI_propertySetByUser_usage",
                          "usage",
                          "regularAggregate",
                        ],
                      },
                    },
                  ],
                  identifier: `${classAccuracyTable}`,
                  idForExpression: `${classAccuracyTable}`,
                },
              ],
              dataRetrievalMode: "liveConnection",
              identifier: "C_newModel",
              label: "newModel",
            },
          },
        },
        {
          id: "model0000017fff1217cd_00000002",
          assetId: "assetId0000017fff1217cd_00000000",
          clientId: "9Table",
          module: {
            xsd: xsd,
            source: {
              id: "StringID",
              jdbc: {
                jdbcUrl: jdbcUrl,
                driverClassName: driverClassName,
                connectionProperties: "OPTIONAL=TRUE;LOGINTIMEOUT=0",
                schema: schema,
              },
              user: user,
              password: password,
            },
            table: {
              name: `${pairWiseClassErrorsTable}`,
              description: `${pairWiseClassErrorsTable}`,
              column: [
                {
                  name: "TRUECLASS",
                  description: "String",
                  datatype: "VARCHAR",
                  nullable: false,
                  label: "True Class",
                  usage: "identifier",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "PREDICTEDCLASS",
                  description: "String",
                  datatype: "VARCHAR",
                  nullable: false,
                  label: "Predicted Class",
                  usage: "identifier",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "CONFIDENCE",
                  description: "String",
                  datatype: "DECIMAL",
                  nullable: false,
                  label: "Confidence",
                  usage: "attribute",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "INPUT",
                  description: "String",
                  datatype: "VARCHAR",
                  nullable: false,
                  label: "Input",
                  usage: "identifier",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
              ],
            },
            label: "Db2 Module",
            identifier: "db2module1Table",
          },
          name: "Pair Wise Class Errors",
          shaping: {
            shapingId: "shaping0000017fff3338d5_00000000",
            embeddedModuleUpToDate: false,
            moserJSON: {
              version: "11.0",
              container: "C",
              useSpec: [
                {
                  identifier: "ES",
                  type: "url",
                  storeID: "baseModule",
                  imports: "*",
                },
              ],
              expressionLocale: "en-us",
              querySubject: [
                {
                  ref: [`ES.${pairWiseClassErrorsTable}`],
                  instanceType: "reference",
                  item: [
                    {
                      queryItem: {
                        usage: "fact",
                        regularAggregate: "total",
                        identifier: "CONFIDENCE",
                        property: [
                          {
                            name: "_MUI_propertySetByUser_usage",
                            value: '"true"',
                          },
                          {
                            name: "_path",
                            value: "2",
                          },
                        ],
                        propertyOverride: [
                          "property._MUI_propertySetByUser_usage",
                          "usage",
                          "regularAggregate",
                        ],
                      },
                    },
                  ],
                  identifier: `${pairWiseClassErrorsTable}`,
                  idForExpression: `${pairWiseClassErrorsTable}`,
                },
              ],
              dataRetrievalMode: "liveConnection",
              identifier: "C_newModel",
              label: "newModel",
            },
          },
        },
        {
          id: "model0000017fff1217cd_00000005",
          assetId: "assetId0000017fff1217cd_00000003",
          clientId: "10Table",
          module: {
            xsd: xsd,
            source: {
              id: "StringID",
              jdbc: {
                jdbcUrl: jdbcUrl,
                driverClassName: driverClassName,
                connectionProperties: "OPTIONAL=TRUE;LOGINTIMEOUT=0",
                schema: schema,
              },
              user: user,
              password: password,
            },
            table: {
              name: `${accuracyVsCoverageTable}`,
              description: `${accuracyVsCoverageTable}`,
              column: [
                {
                  name: "CONFIDENCETHRESHOLD",
                  description: "String",
                  datatype: "DECIMAL",
                  nullable: false,
                  label: "Confidence Threshold",
                  usage: "identifier",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "ACCURACY",
                  description: "String",
                  datatype: "DECIMAL",
                  nullable: false,
                  label: "Accuracy",
                  usage: "attribute",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
                {
                  name: "COVERAGE",
                  description: "String",
                  datatype: "DECIMAL",
                  nullable: false,
                  label: "Coverage",
                  usage: "attribute",
                  regularAggregate: "countDistinct",
                  taxonomyFamily: "cNone",
                },
              ],
            },
            label: "Db2 Module",
            identifier: "db2module1Table",
          },
          name: "Accuracy vs Coverage",
          shaping: {
            shapingId: "shaping0000017fff3a4b9a_00000000",
            embeddedModuleUpToDate: false,
            moserJSON: {
              version: "11.0",
              container: "C",
              useSpec: [
                {
                  identifier: "ES",
                  type: "url",
                  storeID: "baseModule",
                  imports: "*",
                },
              ],
              expressionLocale: "en-us",
              querySubject: [
                {
                  ref: [`ES.${accuracyVsCoverageTable}`],
                  instanceType: "reference",
                  item: [
                    {
                      queryItem: {
                        usage: "fact",
                        regularAggregate: "total",
                        identifier: "CONFIDENCETHRESHOLD",
                        property: [
                          {
                            name: "_MUI_propertySetByUser_usage",
                            value: '"true"',
                          },
                          {
                            name: "_path",
                            value: "0",
                          },
                        ],
                        propertyOverride: [
                          "property._MUI_propertySetByUser_usage",
                          "usage",
                          "regularAggregate",
                        ],
                      },
                    },
                    {
                      queryItem: {
                        usage: "fact",
                        regularAggregate: "total",
                        identifier: "ACCURACY",
                        property: [
                          {
                            name: "_MUI_propertySetByUser_usage",
                            value: '"true"',
                          },
                          {
                            name: "_path",
                            value: "1",
                          },
                        ],
                        propertyOverride: [
                          "property._MUI_propertySetByUser_usage",
                          "usage",
                          "regularAggregate",
                        ],
                      },
                    },
                    {
                      queryItem: {
                        usage: "fact",
                        regularAggregate: "total",
                        identifier: "COVERAGE",
                        property: [
                          {
                            name: "_MUI_propertySetByUser_usage",
                            value: '"true"',
                          },
                          {
                            name: "_path",
                            value: "2",
                          },
                        ],
                        propertyOverride: [
                          "property._MUI_propertySetByUser_usage",
                          "usage",
                          "regularAggregate",
                        ],
                      },
                    },
                  ],
                  identifier: `${accuracyVsCoverageTable}`,
                  idForExpression: `${accuracyVsCoverageTable}`,
                },
              ],
              dataRetrievalMode: "liveConnection",
              identifier: "C_newModel",
              label: "newModel",
            },
          },
        },
      ],
    },
    widgets: {
      model0000017e8d7e356e_00000000: {
        type: "live",
        id: "model0000017e8d7e356e_00000000",
        visId: "com.ibm.vis.rave2bundlestackedcolumn",
        name: {
          translationTable: {},
        },
        data: {
          dataViews: [
            {
              modelRef: "model0000017e7cdcde39_00000002",
              dataItems: [
                {
                  id: "id_675748283",
                  itemId: `${conversationTable}.C_Conversation_Start_T`,
                  itemLabel: "simpleDate",
                  navigationPathId: null,
                },
                {
                  id: "id559722038",
                  itemId: `${conversationTable}.Chat_Count`,
                  itemLabel: "Chat Count",
                  navigationPathId: null,
                },
                {
                  id: "id_1324639312",
                  itemId: `${conversationTable}.Phone_Count`,
                  itemLabel: "Phone Count",
                  navigationPathId: null,
                },
                {
                  id: "_multiMeasuresSeries",
                  itemId: "_multiMeasuresSeries",
                  itemLabel: "Measures group (2)",
                },
              ],
              id: "model0000017e8d803bfb_00000000",
            },
          ],
        },
        visTypeLocked: true,
        slotmapping: {
          slots: [
            {
              name: "categories",
              dataItems: ["id_675748283"],
              layerId: "data",
            },
            {
              name: "color",
              dataItems: ["_multiMeasuresSeries"],
            },
            {
              name: "values",
              dataItems: ["id_1324639312", "id559722038"],
              layerId: "data",
            },
          ],
        },
        properties: [
          {
            id: "widget.legend.size",
            value: "21.2%",
          },
        ],
        localFilters: [],
        queryRefresh: {
          autoRefresh: true,
          unit: "seconds",
          value: "5",
          lastRefreshed: 1649363100285,
        },
      },
      model0000017e9c99bcde_00000000: {
        type: "live",
        id: "model0000017e9c99bcde_00000000",
        visId: "com.ibm.vis.rave2bundlestackedcolumn",
        name: {
          translationTable: {},
        },
        data: {
          dataViews: [
            {
              modelRef: "model0000017e7cdcde39_00000002",
              dataItems: [
                {
                  id: "id_675748283",
                  itemId: `${conversationTable}.C_Conversation_Start_T`,
                  itemLabel: "simpleDate",
                  navigationPathId: null,
                },
                {
                  id: "id289202159",
                  itemId: `${conversationTable}.newUser_1`,
                  itemLabel: "newUser",
                  navigationPathId: null,
                },
                {
                  id: "id_1137690479",
                  itemId: `${conversationTable}.reccurrentUsers`,
                  itemLabel: "reccurrentUsers",
                  navigationPathId: null,
                },
                {
                  id: "_multiMeasuresSeries",
                  itemId: "_multiMeasuresSeries",
                  itemLabel: "Measures group (2)",
                },
              ],
              id: "model0000017e9c99eed8_00000000",
            },
          ],
        },
        visTypeLocked: true,
        slotmapping: {
          slots: [
            {
              name: "categories",
              dataItems: ["id_675748283"],
              layerId: "data",
            },
            {
              name: "color",
              dataItems: ["_multiMeasuresSeries"],
            },
            {
              name: "values",
              dataItems: ["id289202159", "id_1137690479"],
              layerId: "data",
            },
          ],
        },
        properties: [],
        localFilters: [],
        queryRefresh: {
          autoRefresh: true,
          unit: "seconds",
          value: "5",
          lastRefreshed: 1649363100285,
        },
      },
      model0000017ea15b1b5e_00000000: {
        type: "live",
        id: "model0000017ea15b1b5e_00000000",
        visId: "com.ibm.vis.rave2bundlepie",
        titleHtml: {
          translationTable: {
            Default:
              '<div class="widgetTitle" title="" aria-labelledby="model0000017ea15b1b5e_00000000Title" appcues-data-id="widgetTitle"><div class="textArea"><div class="summernote" style="display: none;"></div><div class="note-editor"><div class="note-dropzone">  <div class="note-dropzone-message"></div></div><div class="note-editing-area"><div class="note-handle"><div class="note-control-selection" style="display: none;"><div class="note-control-selection-bg"></div><div class="note-control-holder note-control-nw"></div><div class="note-control-holder note-control-ne"></div><div class="note-control-holder note-control-sw"></div><div class="note-control-sizing note-control-se"></div><div class="note-control-selection-info"></div></div></div><div class="note-editable" contenteditable="true" id="model0000017ea15b1b5e_00000000Title"><span class="textFitted responsiveColor responsiveFontSize responsiveFontFamily"><p style="text-align: center;"><span style="font-size: 16px;"><b>Duração das chamadas telefônicas</b></span></p></span></div></div></div></div></div>',
          },
        },
        name: {
          translationTable: {
            Default: "Duração das chamadas telefônicas",
          },
        },
        data: {
          dataViews: [
            {
              modelRef: "model0000017e7cdcde39_00000002",
              dataItems: [
                {
                  id: "id_984020379",
                  itemId: `${conversationTable}.durationQuartile`,
                  itemLabel: "durationQuartile",
                  aggregate: "countdistinct",
                  navigationPathId: null,
                },
                {
                  id: "id956341870",
                  itemId: `${conversationTable}.Phone_Count`,
                  itemLabel: "Phone Count",
                  navigationPathId: null,
                },
              ],
              id: "model0000017ea16ddeb6_00000000",
              grid: {
                viewOption: "projectedAggregated",
              },
            },
          ],
        },
        visTypeLocked: true,
        slotmapping: {
          slots: [
            {
              name: "categories",
              dataItems: ["id_984020379"],
              dataItemSettings: [],
              layerId: "data",
            },
            {
              name: "values",
              dataItems: ["id956341870"],
              layerId: "data",
            },
          ],
        },
        properties: [
          {
            id: "widget.legend.display",
            value: true,
          },
          {
            id: "widget.legend.position",
            value: "bottom",
          },
          {
            id: "donutRadius",
            value: 1,
          },
          {
            id: "label.percentage",
            value: false,
          },
          {
            id: "labels.visible",
            value: true,
          },
          {
            id: "contrast.label.color",
            value: true,
          },
          {
            id: "widget.legend.font",
            value: null,
          },
          {
            id: "widget.legend.titleVisible",
            value: true,
          },
          {
            id: "categories.title",
            value: {
              translationTable: {
                Default: "Quartis da duração da conversa",
              },
            },
          },
          {
            id: "colorPalette",
            value: "colorPalette2",
          },
        ],
        localFilters: [],
        showTitle: false,
        queryRefresh: {
          autoRefresh: true,
          unit: "seconds",
          value: "5",
          lastRefreshed: 1649363100286,
        },
      },
      model0000017eb1763297_00000003: {
        type: "live",
        id: "model0000017eb1763297_00000003",
        visId: "summary",
        titleHtml: {
          translationTable: {
            Default:
              '<div class="widgetTitle" title="" aria-labelledby="model0000017e97bc3135_00000001Title" appcues-data-id="widgetTitle"><div class="textArea"><div class="summernote" style="display: none;"></div><div class="note-editor"><div class="note-dropzone">  <div class="note-dropzone-message"></div></div><div class="note-editing-area"><div class="note-handle"><div class="note-control-selection"><div class="note-control-selection-bg"></div><div class="note-control-holder note-control-nw"></div><div class="note-control-holder note-control-ne"></div><div class="note-control-holder note-control-sw"></div><div class="note-control-sizing note-control-se"></div><div class="note-control-selection-info"></div></div></div><div class="note-editable" contenteditable="true" id="model0000017e97bc3135_00000001Title"><span class="textFitted responsiveColor responsiveFontSize responsiveFontFamily"><p><span>Enter your title here</span></p></span></div></div></div></div></div>',
          },
        },
        name: {
          translationTable: {
            Default: "Enter your title here",
          },
        },
        data: {
          dataViews: [
            {
              modelRef: "model0000017e7cdcde39_00000002",
              dataItems: [
                {
                  id: "id_2115705388",
                  itemId: `${conversationTable}.C_transferedUsers`,
                  itemLabel: "C_transferedUsers",
                  format: {
                    formatSpec: {
                      type: "percent",
                      timezone: "America/New_York",
                      locale: "en",
                      maximumFractionDigits: 2,
                      minimumFractionDigits: 2,
                      convertPercent: false,
                      useGrouping: false,
                      local: true,
                    },
                  },
                },
              ],
              id: "model0000017e9802483c_00000000",
            },
          ],
        },
        visTypeLocked: true,
        slotmapping: {
          slots: [
            {
              name: "actual",
              dataItems: ["id_2115705388"],
              dataItemSettings: [],
              layerId: "data",
            },
          ],
        },
        properties: [
          {
            id: "showItemLabel",
            value: false,
          },
          {
            id: "baseValueLabel",
            value: {
              translationTable: {},
            },
          },
        ],
        localFilters: [],
        showTitle: false,
        queryRefresh: {
          autoRefresh: true,
          unit: "seconds",
          value: "5",
          lastRefreshed: 1649363100286,
        },
      },
      model0000017eb1766a05_00000003: {
        type: "live",
        id: "model0000017eb1766a05_00000003",
        visId: "summary",
        titleHtml: {
          translationTable: {
            Default:
              '<div class="widgetTitle" title="" aria-labelledby="model0000017e97bc3135_00000001Title" appcues-data-id="widgetTitle"><div class="textArea"><div class="summernote" style="display: none;"></div><div class="note-editor"><div class="note-dropzone">  <div class="note-dropzone-message"></div></div><div class="note-editing-area"><div class="note-handle"><div class="note-control-selection"><div class="note-control-selection-bg"></div><div class="note-control-holder note-control-nw"></div><div class="note-control-holder note-control-ne"></div><div class="note-control-holder note-control-sw"></div><div class="note-control-sizing note-control-se"></div><div class="note-control-selection-info"></div></div></div><div class="note-editable" contenteditable="true" id="model0000017e97bc3135_00000001Title"><span class="textFitted responsiveColor responsiveFontSize responsiveFontFamily"><p><span>Enter your title here</span></p></span></div></div></div></div></div>',
          },
        },
        name: {
          translationTable: {
            Default: "Enter your title here",
          },
        },
        data: {
          dataViews: [
            {
              modelRef: "model0000017e7cdcde39_00000002",
              dataItems: [
                {
                  id: "id308483713",
                  itemId: `${conversationTable}.i_relevant`,
                  itemLabel: "%relevant",
                  format: {
                    formatSpec: {
                      type: "percent",
                      timezone: "America/New_York",
                      locale: "en",
                      maximumFractionDigits: 2,
                      minimumFractionDigits: 2,
                      convertPercent: false,
                      useGrouping: true,
                      local: true,
                    },
                  },
                },
              ],
              id: "model0000017e980f1704_00000000",
            },
          ],
        },
        visTypeLocked: true,
        slotmapping: {
          slots: [
            {
              name: "actual",
              dataItems: ["id308483713"],
              dataItemSettings: [],
              layerId: "data",
            },
          ],
        },
        properties: [
          {
            id: "showItemLabel",
            value: false,
          },
          {
            id: "baseValueLabel",
            value: {
              translationTable: {
                Default: "Relevantes",
              },
            },
          },
        ],
        localFilters: [],
        showTitle: false,
        queryRefresh: {
          autoRefresh: true,
          unit: "seconds",
          value: "5",
          lastRefreshed: 1649363100286,
        },
      },
      model0000017eb1773a05_00000004: {
        type: "live",
        id: "model0000017eb1773a05_00000004",
        visId: "com.ibm.vis.rave2line",
        name: {
          translationTable: {},
        },
        data: {
          dataViews: [
            {
              modelRef: "model0000017e7cdcde39_00000002",
              dataItems: [
                {
                  id: "id_675748283",
                  itemId: `${conversationTable}.C_Conversation_Start_T`,
                  itemLabel: "simpleDate",
                  navigationPathId: null,
                },
                {
                  id: "id1182478968",
                  itemId: `${conversationTable}.CONVERSATIONID`,
                  itemLabel: "ID Conversation",
                  aggregate: "count",
                  navigationPathId: null,
                },
              ],
              id: "model0000017e8d4ea67b_00000000",
            },
          ],
        },
        visTypeLocked: true,
        slotmapping: {
          slots: [
            {
              name: "categories",
              dataItems: ["id_675748283"],
              layerId: "data",
            },
            {
              name: "values",
              dataItems: ["id1182478968"],
              dataItemSettings: [],
              layerId: "data",
            },
          ],
        },
        properties: [
          {
            id: "itemAxis.title",
            value: {
              translationTable: {
                Default: "Date",
              },
            },
          },
          {
            id: "valueAxis.title",
            value: {
              translationTable: {
                Default: "Conversations",
              },
            },
          },
        ],
        localFilters: [],
        queryRefresh: {
          autoRefresh: true,
          unit: "seconds",
          value: "5",
          lastRefreshed: 1649363100286,
        },
      },
      model0000017eb1784e05_00000000: {
        type: "live",
        id: "model0000017eb1784e05_00000000",
        visId: "com.ibm.vis.rave2bundlepie",
        titleHtml: {
          translationTable: {
            Default:
              '<div class="widgetTitle titleShown" title="" aria-labelledby="model0000017e8d2998ce_00000000Title" appcues-data-id="widgetTitle"><div class="textArea hidden"><div class="summernote" style="display: none;"></div><div class="note-editor"><div class="note-dropzone">  <div class="note-dropzone-message"></div></div><div class="note-editing-area"><div class="note-handle"><div class="note-control-selection" style="display: none;"><div class="note-control-selection-bg"></div><div class="note-control-holder note-control-nw"></div><div class="note-control-holder note-control-ne"></div><div class="note-control-holder note-control-sw"></div><div class="note-control-sizing note-control-se"></div><div class="note-control-selection-info"></div></div></div><div class="note-editable" contenteditable="false" id="model0000017eb1784e05_00000000Title"><span class="textFitted responsiveColor responsiveFontSize responsiveFontFamily"><p><br></p></span></div></div></div></div></div>',
          },
        },
        name: {
          translationTable: {},
        },
        data: {
          dataViews: [
            {
              modelRef: "model0000017e7cdcde38_00000002",
              dataItems: [
                {
                  id: "id_202502243",
                  itemId: `${logsTable}.FIRSTINTENT`,
                  itemLabel: "First Intent",
                  format: {
                    formatSpec: {
                      type: "text",
                      timezone: "America/New_York",
                      locale: "en",
                      local: true,
                    },
                  },
                },
                {
                  id: "id731071238",
                  itemId: `${logsTable}.FIRSTINTENT`,
                  itemLabel: "First Intent",
                  navigationPathId: null,
                },
              ],
              id: "model0000017e8d29c486_00000001",
            },
          ],
        },
        visTypeLocked: true,
        slotmapping: {
          slots: [
            {
              name: "categories",
              dataItems: ["id_202502243"],
              dataItemSettings: [],
              layerId: "data",
            },
            {
              name: "values",
              dataItems: ["id731071238"],
              dataItemSettings: [],
              layerId: "data",
            },
          ],
        },
        properties: [
          {
            id: "widget.legend.display",
            value: true,
          },
          {
            id: "widget.legend.position",
            value: "bottom",
          },
          {
            id: "label.percentage",
            value: true,
          },
          {
            id: "labelLocation",
            value: "centerHorizontal",
          },
          {
            id: "contrast.label.color",
            value: true,
          },
          {
            id: "label.shadow",
            value: false,
          },
          {
            id: "widget.legend.size",
            value: "36%",
          },
          {
            id: "widget.legend.titleVisible",
            value: false,
          },
          {
            id: "categories.title",
            value: {
              translationTable: {},
            },
          },
        ],
        localFilters: [
          {
            id: `${logsTable}.FIRSTINTENT`,
            columnId: `${logsTable}.FIRSTINTENT`,
            values: [
              {
                u: `${logsTable}.FIRSTINTENT->[]`,
                d: "",
                value: `${logsTable}.FIRSTINTENT->[]`,
                label: "",
                selected: true,
              },
            ],
            excludedValues: [],
            operator: "notin",
            type: null,
            binsLabel: "First Intent",
          },
        ],
        showTitle: false,
        queryRefresh: {
          autoRefresh: true,
          unit: "seconds",
          value: "5",
          lastRefreshed: 1649363100482,
        },
      },
      model0000017eb17a6940_00000003: {
        type: "live",
        id: "model0000017eb17a6940_00000003",
        visId: "summary",
        name: {
          translationTable: {},
        },
        data: {
          dataViews: [
            {
              modelRef: "model0000017e7cdcde39_00000002",
              dataItems: [
                {
                  id: "id_250085716",
                  itemId: `${conversationTable}.CONVERSATIONID`,
                  itemLabel: "ID Conversation",
                  aggregate: "countdistinct",
                  navigationPathId: null,
                },
              ],
              id: "model0000017e9ca864d8_00000000",
            },
          ],
        },
        visTypeLocked: true,
        slotmapping: {
          slots: [
            {
              name: "actual",
              dataItems: ["id_250085716"],
              dataItemSettings: [],
              layerId: "data",
            },
          ],
        },
        properties: [
          {
            id: "showItemLabel",
            value: false,
          },
          {
            id: "baseValueLabel",
            value: {
              translationTable: {
                Default: "Conversas",
              },
            },
          },
        ],
        localFilters: [],
        queryRefresh: {
          autoRefresh: true,
          unit: "seconds",
          value: "5",
          lastRefreshed: 1649363100287,
        },
      },
      model0000017eb17b2fc4_00000000: {
        id: "model0000017eb17b2fc4_00000000",
        type: "text",
        name: {
          translationTable: {
            Default: "Qtd de Conversas por Dia",
          },
        },
        content: {
          translationTable: {
            Default:
              '<div class="staticContent fill-transparent border-color1" dir="auto" style=""><div class="summernote" style="display: none;"></div><div class="note-editor"><div class="note-dropzone">  <div class="note-dropzone-message"></div></div><div class="note-editing-area"><div class="note-handle"><div class="note-control-selection"><div class="note-control-selection-bg"></div><div class="note-control-holder note-control-nw"></div><div class="note-control-holder note-control-ne"></div><div class="note-control-holder note-control-sw"></div><div class="note-control-sizing note-control-se"></div><div class="note-control-selection-info"></div></div></div><div class="note-editable" contenteditable="false" id="model0000017eb17b2fc4_00000000Editable"><span class="textFitted responsiveColor responsiveFontSize responsiveFontFamily" style=""><p style="text-align: center;"><font face="Montserrat-Regular" size="3">Qtd de Conversas por Dia</font></p></span></div></div></div></div>',
          },
        },
        isResponsive: false,
        placeholder: {
          showIcon: false,
          text: "Enter your text here",
        },
        visTypeLocked: true,
        fillColor: "transparent",
        borderColor: "color1",
      },
      model0000017eb17c87dd_00000000: {
        id: "model0000017eb17c87dd_00000000",
        type: "text",
        name: {
          translationTable: {
            Default: "Qtd total de Conversas",
          },
        },
        content: {
          translationTable: {
            Default:
              '<div class="staticContent border-color1" dir="auto" style=""><div class="summernote" style="display: none;"></div><div class="note-editor"><div class="note-dropzone">  <div class="note-dropzone-message"></div></div><div class="note-editing-area"><div class="note-handle"><div class="note-control-selection"><div class="note-control-selection-bg"></div><div class="note-control-holder note-control-nw"></div><div class="note-control-holder note-control-ne"></div><div class="note-control-holder note-control-sw"></div><div class="note-control-sizing note-control-se"></div><div class="note-control-selection-info"></div></div></div><div class="note-editable" contenteditable="false" id="model0000017eb17c87dd_00000000Editable"><span class="textFitted responsiveColor responsiveFontSize responsiveFontFamily" style=""><p style="text-align: center;"><font face="Montserrat-Regular" size="3">Qtd total de Conversas</font></p></span></div></div></div></div>',
          },
        },
        isResponsive: false,
        placeholder: {
          showIcon: false,
          text: "Enter your text here",
        },
        visTypeLocked: true,
        borderColor: "color1",
      },
      model0000017eb17d1e22_00000000: {
        id: "model0000017eb17d1e22_00000000",
        type: "text",
        name: {
          translationTable: {
            Default: "Qtd de Conversas Transferidas",
          },
        },
        content: {
          translationTable: {
            Default:
              '<div class="staticContent border-color1" dir="auto" style=""><div class="summernote" style="display: none;"></div><div class="note-editor"><div class="note-dropzone">  <div class="note-dropzone-message"></div></div><div class="note-editing-area"><div class="note-handle"><div class="note-control-selection"><div class="note-control-selection-bg"></div><div class="note-control-holder note-control-nw"></div><div class="note-control-holder note-control-ne"></div><div class="note-control-holder note-control-sw"></div><div class="note-control-sizing note-control-se"></div><div class="note-control-selection-info"></div></div></div><div class="note-editable" contenteditable="false" id="model0000017eb17d1e22_00000000Editable"><span class="textFitted responsiveColor responsiveFontSize responsiveFontFamily" style=""><p style="text-align: center;"><font face="Montserrat-Regular" size="3"><span style="font-size: 16px;">Qtd de Conversas Transferidas</span></font><span style="font-size: 16px;">﻿</span></p></span></div></div></div></div>',
          },
        },
        isResponsive: false,
        placeholder: {
          showIcon: false,
          text: "Enter your text here",
        },
        visTypeLocked: true,
        borderColor: "color1",
      },
      model0000017eb17d5a7c_00000000: {
        id: "model0000017eb17d5a7c_00000000",
        type: "text",
        name: {
          translationTable: {
            Default: "Qtd de Conversas Relevantes",
          },
        },
        content: {
          translationTable: {
            Default:
              '<div class="staticContent border-color1" dir="auto" style=""><div class="summernote" style="display: none;"></div><div class="note-editor"><div class="note-dropzone">  <div class="note-dropzone-message"></div></div><div class="note-editing-area"><div class="note-handle"><div class="note-control-selection"><div class="note-control-selection-bg"></div><div class="note-control-holder note-control-nw"></div><div class="note-control-holder note-control-ne"></div><div class="note-control-holder note-control-sw"></div><div class="note-control-sizing note-control-se"></div><div class="note-control-selection-info"></div></div></div><div class="note-editable" contenteditable="false" id="model0000017eb17d5a7c_00000000Editable"><span class="textFitted responsiveColor responsiveFontSize responsiveFontFamily" style=""><p style="text-align: center;"><font face="Montserrat-Regular" size="3">Qtd de Conversas Relevantes</font></p></span></div></div></div></div>',
          },
        },
        isResponsive: false,
        placeholder: {
          showIcon: false,
          text: "Enter your text here",
        },
        visTypeLocked: true,
        borderColor: "color1",
      },
      model0000017eb17e0f18_00000000: {
        id: "model0000017eb17e0f18_00000000",
        type: "text",
        name: {
          translationTable: {
            Default: "Intents mais Acessadas",
          },
        },
        content: {
          translationTable: {
            Default:
              '<div class="staticContent border-color1" dir="auto" style=""><div class="summernote" style="display: none;"></div><div class="note-editor"><div class="note-dropzone">  <div class="note-dropzone-message"></div></div><div class="note-editing-area"><div class="note-handle"><div class="note-control-selection"><div class="note-control-selection-bg"></div><div class="note-control-holder note-control-nw"></div><div class="note-control-holder note-control-ne"></div><div class="note-control-holder note-control-sw"></div><div class="note-control-sizing note-control-se"></div><div class="note-control-selection-info"></div></div></div><div class="note-editable" contenteditable="false" id="model0000017eb17e0f18_00000000Editable"><span class="textFitted responsiveColor responsiveFontSize responsiveFontFamily" style=""><p style="text-align: center;"><font face="Montserrat-Regular" size="3">Intents mais Acessadas</font></p></span></div></div></div></div>',
          },
        },
        isResponsive: false,
        placeholder: {
          showIcon: false,
          text: "Enter your text here",
        },
        visTypeLocked: true,
        borderColor: "color1",
      },
      model0000017eb17e688f_00000000: {
        id: "model0000017eb17e688f_00000000",
        type: "shape",
        name: "Line",
        content:
          '<svg xmlns="http://www.w3.org/2000/svg" version="1.1"  class="staticContent" height="100%" width="100%" preserveAspectRatio="none"  viewBox="0 0 100 100"><g xmlns="http://www.w3.org/2000/svg" vector-effect="non-scaling-stroke"><path stroke="currentColor" stroke-width="3" vector-effect="non-scaling-stroke" d="M0 50h100"/><path style="fill:none!important;stroke:none!important;pointer-events:all!important" vector-effect="non-scaling-stroke" d="M0 30h100v40H0z"/></g></svg>',
        borderColor: "color5",
        fillColor: "color5",
        visTypeLocked: true,
      },
      model0000017eb1823fd5_00000000: {
        id: "model0000017eb1823fd5_00000000",
        type: "text",
        name: {
          translationTable: {
            Default: "Qtd de Usuários Únicos",
          },
        },
        content: {
          translationTable: {
            Default:
              '<div class="staticContent border-color1 fill-transparent" dir="auto" style=""><div class="summernote" style="display: none;"></div><div class="note-editor"><div class="note-dropzone">  <div class="note-dropzone-message"></div></div><div class="note-editing-area"><div class="note-handle"><div class="note-control-selection"><div class="note-control-selection-bg"></div><div class="note-control-holder note-control-nw"></div><div class="note-control-holder note-control-ne"></div><div class="note-control-holder note-control-sw"></div><div class="note-control-sizing note-control-se"></div><div class="note-control-selection-info"></div></div></div><div class="note-editable" contenteditable="false" id="model0000017eb1823fd5_00000000Editable"><span class="textFitted responsiveColor responsiveFontSize responsiveFontFamily" style=""><p style="text-align: center;"><font face="Montserrat-Regular" size="3">Qtd de Usuários Únicos</font></p></span></div></div></div></div>',
          },
        },
        isResponsive: false,
        placeholder: {
          showIcon: false,
          text: "Enter your text here",
        },
        visTypeLocked: true,
        fillColor: "transparent",
        borderColor: "color1",
      },
      model0000017eb1833152_00000003: {
        type: "live",
        id: "model0000017eb1833152_00000003",
        visId: "summary",
        name: {
          translationTable: {},
        },
        data: {
          dataViews: [
            {
              modelRef: "model0000017e7cdcde38_00000002",
              dataItems: [
                {
                  id: "id1362727861",
                  itemId: `${logsTable}.Média_mensagem_conversa`,
                  itemLabel: "Média mensagem/conversa",
                  format: {
                    formatSpec: {
                      type: "number",
                      timezone: "America/New_York",
                      locale: "en",
                      maximumFractionDigits: 2,
                      minimumFractionDigits: 2,
                      useGrouping: true,
                      local: true,
                    },
                  },
                },
              ],
              id: "model0000017eb1849799_00000000",
              grid: {
                viewOption: "projectedAggregated",
              },
            },
          ],
        },
        visTypeLocked: true,
        slotmapping: {
          slots: [
            {
              name: "actual",
              dataItems: ["id1362727861"],
              dataItemSettings: [],
              layerId: "data",
            },
          ],
        },
        properties: [
          {
            id: "showItemLabel",
            value: false,
          },
          {
            id: "baseValueLabel",
            value: {
              translationTable: {
                Default: "Conversas",
              },
            },
          },
        ],
        localFilters: [],
        queryRefresh: {
          autoRefresh: true,
          unit: "seconds",
          value: "5",
          lastRefreshed: 1649363100482,
        },
      },
      model0000017eb18349fb_00000002: {
        type: "live",
        id: "model0000017eb18349fb_00000002",
        visId: "summary",
        name: {
          translationTable: {},
        },
        data: {
          dataViews: [
            {
              modelRef: "model0000017e7cdcde38_00000002",
              dataItems: [
                {
                  id: "id1305286256",
                  itemId: `${logsTable}.LOGID`,
                  itemLabel: "ID Log",
                },
              ],
              id: "model0000017eb183f57b_00000000",
            },
          ],
        },
        visTypeLocked: true,
        slotmapping: {
          slots: [
            {
              name: "actual",
              dataItems: ["id1305286256"],
              dataItemSettings: [],
              layerId: "data",
            },
          ],
        },
        properties: [
          {
            id: "showItemLabel",
            value: false,
          },
          {
            id: "baseValueLabel",
            value: {
              translationTable: {
                Default: "Conversas",
              },
            },
          },
        ],
        localFilters: [],
        queryRefresh: {
          autoRefresh: true,
          unit: "seconds",
          value: "5",
          lastRefreshed: 1649363100482,
        },
      },
      model0000017eb18597dc_00000000: {
        id: "model0000017eb18597dc_00000000",
        type: "text",
        name: {
          translationTable: {
            Default: "Qtd total de Mensagens",
          },
        },
        content: {
          translationTable: {
            Default:
              '<div class="staticContent border-color1" dir="auto" style=""><div class="summernote" style="display: none;"></div><div class="note-editor"><div class="note-dropzone">  <div class="note-dropzone-message"></div></div><div class="note-editing-area"><div class="note-handle"><div class="note-control-selection"><div class="note-control-selection-bg"></div><div class="note-control-holder note-control-nw"></div><div class="note-control-holder note-control-ne"></div><div class="note-control-holder note-control-sw"></div><div class="note-control-sizing note-control-se"></div><div class="note-control-selection-info"></div></div></div><div class="note-editable" contenteditable="false" id="model0000017eb18597dc_00000000Editable"><span class="textFitted responsiveColor responsiveFontSize responsiveFontFamily" style=""><p style="text-align: center;"><font face="Montserrat-Regular" size="3">Qtd total de Mensagens</font></p></span></div></div></div></div>',
          },
        },
        isResponsive: false,
        placeholder: {
          showIcon: false,
          text: "Enter your text here",
        },
        visTypeLocked: true,
        borderColor: "color1",
      },
      model0000017eb1859b57_00000000: {
        id: "model0000017eb1859b57_00000000",
        type: "text",
        name: {
          translationTable: {
            Default: "Média de Mensagens por Conversa",
          },
        },
        content: {
          translationTable: {
            Default:
              '<div class="staticContent border-color1" dir="auto" style=""><div class="summernote" style="display: none;"></div><div class="note-editor"><div class="note-dropzone">  <div class="note-dropzone-message"></div></div><div class="note-editing-area"><div class="note-handle"><div class="note-control-selection"><div class="note-control-selection-bg"></div><div class="note-control-holder note-control-nw"></div><div class="note-control-holder note-control-ne"></div><div class="note-control-holder note-control-sw"></div><div class="note-control-sizing note-control-se"></div><div class="note-control-selection-info"></div></div></div><div class="note-editable" contenteditable="false" id="model0000017eb1859b57_00000000Editable"><span class="textFitted responsiveColor responsiveFontSize responsiveFontFamily" style=""><p style="text-align: center;"><font face="Montserrat-Regular" size="3">Média de Mensagens por Conversa</font></p></span></div></div></div></div>',
          },
        },
        isResponsive: false,
        placeholder: {
          showIcon: false,
          text: "Enter your text here",
        },
        visTypeLocked: true,
        borderColor: "color1",
      },
      model0000017eb1897146_00000000: {
        id: "model0000017eb1897146_00000000",
        type: "text",
        name: {
          translationTable: {
            Default: "Quartil das ligações telefônicas com base em sua duração",
          },
        },
        content: {
          translationTable: {
            Default:
              '<div class="staticContent border-color1" dir="auto" style=""><div class="summernote" style="display: none;"></div><div class="note-editor"><div class="note-dropzone">  <div class="note-dropzone-message"></div></div><div class="note-editing-area"><div class="note-handle"><div class="note-control-selection"><div class="note-control-selection-bg"></div><div class="note-control-holder note-control-nw"></div><div class="note-control-holder note-control-ne"></div><div class="note-control-holder note-control-sw"></div><div class="note-control-sizing note-control-se"></div><div class="note-control-selection-info"></div></div></div><div class="note-editable" contenteditable="false" id="model0000017eb1897146_00000000Editable"><span class="textFitted responsiveColor responsiveFontSize responsiveFontFamily" style=""><p style="text-align: center;"><font face="Montserrat-Regular" size="3">Quartil das ligações telefônicas com base em sua duração</font></p></span></div></div></div></div>',
          },
        },
        isResponsive: false,
        placeholder: {
          showIcon: false,
          text: "Enter your text here",
        },
        visTypeLocked: true,
        borderColor: "color1",
      },
      model0000017eb19252a9_00000000: {
        id: "model0000017eb19252a9_00000000",
        type: "text",
        name: {
          translationTable: {
            Default: "Canal da conversa",
          },
        },
        content: {
          translationTable: {
            Default:
              '<div class="staticContent border-color1" dir="auto" style=""><div class="summernote" style="display: none;"></div><div class="note-editor"><div class="note-dropzone">  <div class="note-dropzone-message"></div></div><div class="note-editing-area"><div class="note-handle"><div class="note-control-selection"><div class="note-control-selection-bg"></div><div class="note-control-holder note-control-nw"></div><div class="note-control-holder note-control-ne"></div><div class="note-control-holder note-control-sw"></div><div class="note-control-sizing note-control-se"></div><div class="note-control-selection-info"></div></div></div><div class="note-editable" contenteditable="false" id="model0000017eb19252a9_00000000Editable"><span class="textFitted responsiveColor responsiveFontSize responsiveFontFamily" style=""><p style="text-align: center;"><font face="Montserrat-Regular" size="3">Canal da conversa</font></p></span></div></div></div></div>',
          },
        },
        isResponsive: false,
        placeholder: {
          showIcon: false,
          text: "Enter your text here",
        },
        visTypeLocked: true,
        borderColor: "color1",
      },
      model0000017eb192cbb7_00000000: {
        id: "model0000017eb192cbb7_00000000",
        type: "text",
        name: {
          translationTable: {
            Default: "Recorrência do usuário",
          },
        },
        content: {
          translationTable: {
            Default:
              '<div class="staticContent border-color1" dir="auto" style=""><div class="summernote" style="display: none;"></div><div class="note-editor"><div class="note-dropzone">  <div class="note-dropzone-message"></div></div><div class="note-editing-area"><div class="note-handle"><div class="note-control-selection"><div class="note-control-selection-bg"></div><div class="note-control-holder note-control-nw"></div><div class="note-control-holder note-control-ne"></div><div class="note-control-holder note-control-sw"></div><div class="note-control-sizing note-control-se"></div><div class="note-control-selection-info"></div></div></div><div class="note-editable" contenteditable="false" id="model0000017eb192cbb7_00000000Editable"><span class="textFitted responsiveColor responsiveFontSize responsiveFontFamily" style=""><p style="text-align: center;"><font face="Montserrat-Regular" size="3">Recorrência do usuário</font></p></span></div></div></div></div>',
          },
        },
        isResponsive: false,
        placeholder: {
          showIcon: false,
          text: "Enter your text here",
        },
        visTypeLocked: true,
        borderColor: "color1",
      },
      model0000017ec0f7adef_00000003: {
        type: "live",
        id: "model0000017ec0f7adef_00000003",
        visId: "summary",
        name: {
          translationTable: {},
        },
        data: {
          dataViews: [
            {
              modelRef: "model0000017e7cdcde39_00000002",
              dataItems: [
                {
                  id: "id_838608076",
                  itemId: `${conversationTable}.IDUSER`,
                  itemLabel: "ID User",
                  navigationPathId: null,
                },
              ],
              id: "model0000017ec0f7fd50_00000000",
            },
          ],
        },
        visTypeLocked: true,
        slotmapping: {
          slots: [
            {
              name: "actual",
              dataItems: ["id_838608076"],
              dataItemSettings: [],
              layerId: "data",
            },
          ],
        },
        properties: [
          {
            id: "showItemLabel",
            value: false,
          },
          {
            id: "baseValueLabel",
            value: {
              translationTable: {
                Default: "Conversas",
              },
            },
          },
        ],
        localFilters: [],
        queryRefresh: {
          autoRefresh: true,
          unit: "seconds",
          value: "5",
          lastRefreshed: 1649363100287,
        },
      },
      model0000017ec0fab020_00000000: {
        id: "model0000017ec0fab020_00000000",
        type: "shape",
        name: "Line",
        content:
          '<svg xmlns="http://www.w3.org/2000/svg" version="1.1"  class="staticContent" height="100%" width="100%" preserveAspectRatio="none"  viewBox="0 0 100 100"><g xmlns="http://www.w3.org/2000/svg" vector-effect="non-scaling-stroke"><path stroke="currentColor" stroke-width="3" vector-effect="non-scaling-stroke" d="M0 50h100"/><path style="fill:none!important;stroke:none!important;pointer-events:all!important" vector-effect="non-scaling-stroke" d="M0 30h100v40H0z"/></g></svg>',
        borderColor: "color5",
        fillColor: "transparent",
        visTypeLocked: true,
      },
      model0000017ec0fdf257_00000002: {
        type: "live",
        id: "model0000017ec0fdf257_00000002",
        visId: "com.ibm.vis.rave2bundlecolumn",
        name: {
          translationTable: {},
        },
        data: {
          dataViews: [
            {
              modelRef: "model0000017e7cdcde39_00000002",
              dataItems: [
                {
                  id: "id895248692",
                  itemId: `${conversationTable}.noFeedback`,
                  itemLabel: "noFeedback",
                },
                {
                  id: "id436348580",
                  itemId: `${conversationTable}.perfectFeedback`,
                  itemLabel: "perfectFeedback",
                },
                {
                  id: "id1265902768",
                  itemId: `${conversationTable}.goodFeedback`,
                  itemLabel: "goodFeedback",
                  navigationPathId: null,
                },
                {
                  id: "id_294592536",
                  itemId: `${conversationTable}.mediumFeedback`,
                  itemLabel: "mediumFeedback",
                  navigationPathId: null,
                },
                {
                  id: "id245428276",
                  itemId: `${conversationTable}.C_Conversation_Feedbac`,
                  itemLabel: "badFeedback",
                  navigationPathId: null,
                },
                {
                  id: "_multiMeasuresSeries",
                  itemId: "_multiMeasuresSeries",
                  itemLabel: "Measures group (5)",
                },
              ],
              id: "model0000017e9ce1cf25_00000000",
              grid: {
                viewOption: "projectedAggregated",
              },
            },
          ],
        },
        visTypeLocked: true,
        slotmapping: {
          slots: [
            {
              name: "color",
              dataItems: ["_multiMeasuresSeries"],
              dataItemSettings: [],
            },
            {
              name: "values",
              dataItems: [
                "id895248692",
                "id245428276",
                "id_294592536",
                "id1265902768",
                "id436348580",
              ],
              dataItemSettings: [],
              layerId: "data",
            },
          ],
        },
        properties: [
          {
            id: "valueLabels.location",
            value: "outsideEnd",
          },
          {
            id: "valueLabels.visible",
            value: true,
          },
          {
            id: "color.title",
            value: {
              translationTable: {
                Default: "Feedback",
              },
            },
          },
        ],
        localFilters: [],
        queryRefresh: {
          autoRefresh: true,
          unit: "seconds",
          value: "5",
          lastRefreshed: 1649363100287,
        },
      },
      model0000017ec0feae2a_00000000: {
        id: "model0000017ec0feae2a_00000000",
        type: "text",
        name: {
          translationTable: {
            Default: "Feedback fornecido pelo cliente",
          },
        },
        content: {
          translationTable: {
            Default:
              '<div class="staticContent border-color1" dir="auto" style=""><div class="summernote" style="display: none;"></div><div class="note-editor"><div class="note-dropzone">  <div class="note-dropzone-message"></div></div><div class="note-editing-area"><div class="note-handle"><div class="note-control-selection"><div class="note-control-selection-bg"></div><div class="note-control-holder note-control-nw"></div><div class="note-control-holder note-control-ne"></div><div class="note-control-holder note-control-sw"></div><div class="note-control-sizing note-control-se"></div><div class="note-control-selection-info"></div></div></div><div class="note-editable" contenteditable="false" id="model0000017ec0feae2a_00000000Editable"><span class="textFitted responsiveColor responsiveFontSize responsiveFontFamily" style=""><p style="text-align: center;"><font face="Montserrat-Regular" size="3">Feedback fornecido pelo cliente</font></p></span></div></div></div></div>',
          },
        },
        isResponsive: false,
        placeholder: {
          showIcon: false,
          text: "Enter your text here",
        },
        visTypeLocked: true,
        borderColor: "color1",
      },
      model0000017fff18ef93_00000000: {
        type: "live",
        id: "model0000017fff18ef93_00000000",
        visId: "com.ibm.vis.rave2bundlecolumn",
        titleHtml: {
          translationTable: {
            Default:
              '<div class="widgetTitle" title="" aria-labelledby="model0000017fff18ef93_00000000Title" appcues-data-id="widgetTitle"><div class="textArea"><div class="summernote" style="display: none;"></div><div class="note-editor"><div class="note-dropzone">  <div class="note-dropzone-message"></div></div><div class="note-editing-area"><div class="note-handle"><div class="note-control-selection" style="display: none;"><div class="note-control-selection-bg"></div><div class="note-control-holder note-control-nw"></div><div class="note-control-holder note-control-ne"></div><div class="note-control-holder note-control-sw"></div><div class="note-control-sizing note-control-se"></div><div class="note-control-selection-info"></div></div></div><div class="note-editable" contenteditable="false" id="model0000017fff18ef93_00000000Title"><span class="textFitted responsiveColor responsiveFontSize responsiveFontFamily"><p>Distribuição de classes</p></span></div></div></div></div></div>',
          },
        },
        name: {
          translationTable: {
            Default: "Distribuição de classes",
          },
        },
        data: {
          dataViews: [
            {
              modelRef: "model0000017fff1217cc_00000002",
              dataItems: [
                {
                  id: "id1448369390",
                  itemId: `${classDistributionTable}.INTENT`,
                  itemLabel: "Intent",
                  navigationPathId: null,
                },
                {
                  id: "id1277556642",
                  itemId: `${classDistributionTable}.COUNT_`,
                  itemLabel: "Count",
                  selection: [
                    {
                      operation: "order",
                      sort: {
                        type: "desc",
                        priority: 0,
                        by: "caption",
                        custom: {},
                      },
                    },
                  ],
                  navigationPathId: null,
                },
              ],
              id: "model0000017fff18fbde_00000000",
            },
          ],
        },
        visTypeLocked: true,
        slotmapping: {
          slots: [
            {
              name: "categories",
              dataItems: ["id1448369390"],
              layerId: "data",
            },
            {
              name: "values",
              dataItems: ["id1277556642"],
              dataItemSettings: [],
              layerId: "data",
            },
          ],
        },
        properties: [],
        localFilters: [],
        showTitle: true,
      },
      model0000017fff1b9dfc_00000004: {
        type: "live",
        id: "model0000017fff1b9dfc_00000004",
        visId: "com.ibm.vis.rave2bundlecolumn",
        titleHtml: {
          translationTable: {
            Default:
              '<div class="widgetTitle" title="" aria-labelledby="model0000017fff1b9dfc_00000004Title" appcues-data-id="widgetTitle"><div class="textArea"><div class="summernote" style="display: none;"></div><div class="note-editor"><div class="note-dropzone">  <div class="note-dropzone-message"></div></div><div class="note-editing-area"><div class="note-handle"><div class="note-control-selection" style="display: none;"><div class="note-control-selection-bg"></div><div class="note-control-holder note-control-nw"></div><div class="note-control-holder note-control-ne"></div><div class="note-control-holder note-control-sw"></div><div class="note-control-sizing note-control-se"></div><div class="note-control-selection-info"></div></div></div><div class="note-editable" contenteditable="false" id="model0000017fff1b9dfc_00000004Title"><span class="textFitted responsiveColor responsiveFontSize responsiveFontFamily"><p><span>Precisão @K</span></p></span></div></div></div></div></div>',
          },
        },
        name: {
          translationTable: {
            Default: "Precisão @K",
          },
        },
        data: {
          dataViews: [
            {
              modelRef: "model0000017fff1217cc_00000005",
              dataItems: [
                {
                  id: "id1010568375",
                  itemId: `${precisionAtKTable}.K`,
                  itemLabel: "K",
                  navigationPathId: null,
                },
                {
                  id: "id1973712085",
                  itemId: `${precisionAtKTable}.PRECISION_`,
                  itemLabel: "Precision",
                  aggregate: "avg",
                  navigationPathId: null,
                },
              ],
              id: "model0000017fff1b1598_00000000",
            },
          ],
        },
        visTypeLocked: true,
        slotmapping: {
          slots: [
            {
              name: "categories",
              dataItems: ["id1010568375"],
              layerId: "data",
            },
            {
              name: "values",
              dataItems: ["id1973712085"],
              dataItemSettings: [],
              layerId: "data",
            },
          ],
        },
        properties: [],
        localFilters: [],
        showTitle: true,
      },
      model0000017fff1d32bf_00000000: {
        type: "live",
        id: "model0000017fff1d32bf_00000000",
        visId: "com.ibm.vis.ravescatter",
        titleHtml: {
          translationTable: {
            Default:
              '<div class="widgetTitle" title="" aria-labelledby="model0000017fff1d32bf_00000000Title" appcues-data-id="widgetTitle"><div class="textArea"><div class="summernote" style="display: none;"></div><div class="note-editor"><div class="note-dropzone">  <div class="note-dropzone-message"></div></div><div class="note-editing-area"><div class="note-handle"><div class="note-control-selection" style="display: none;"><div class="note-control-selection-bg"></div><div class="note-control-holder note-control-nw"></div><div class="note-control-holder note-control-ne"></div><div class="note-control-holder note-control-sw"></div><div class="note-control-sizing note-control-se"></div><div class="note-control-selection-info"></div></div></div><div class="note-editable" contenteditable="true" id="model0000017fff1d32bf_00000000Title"><span class="textFitted responsiveColor responsiveFontSize responsiveFontFamily"><p>Ocorrência vs. Precisão por Intenção</p></span></div></div></div></div></div>',
          },
        },
        name: {
          translationTable: {
            Default: "Ocorrência vs. Precisão por Intenção",
          },
        },
        data: {
          dataViews: [
            {
              modelRef: "model0000017fff1217cc_00000008",
              dataItems: [
                {
                  id: "id676329342",
                  itemId: `${classAccuracyTable}.PRECISION_`,
                  itemLabel: "Precision",
                  navigationPathId: null,
                },
                {
                  id: "id1672338317",
                  itemId: `${classAccuracyTable}.COUNT_`,
                  itemLabel: "Count",
                  navigationPathId: null,
                },
                {
                  id: "id1668976708",
                  itemId: `${classAccuracyTable}.CLASS_`,
                  itemLabel: "Class",
                  navigationPathId: null,
                },
                {
                  id: "id71827175",
                  itemId: `${classAccuracyTable}.CLASS_`,
                  itemLabel: "Class",
                  navigationPathId: null,
                },
              ],
              id: "model0000017fff1e9d05_00000000",
            },
          ],
        },
        visTypeLocked: true,
        slotmapping: {
          slots: [
            {
              name: "x",
              dataItems: ["id1672338317"],
              dataItemSettings: [],
              layerId: "data",
            },
            {
              name: "y",
              dataItems: ["id676329342"],
              layerId: "data",
            },
            {
              name: "categories",
              dataItems: ["id1668976708"],
              layerId: "data",
            },
            {
              name: "color",
              dataItems: ["id71827175"],
              layerId: "data",
            },
          ],
        },
        properties: [
          {
            id: "labels.visible",
            value: false,
          },
          {
            id: "label.shadow",
            value: false,
          },
          {
            id: "widget.legend.size",
            value: "14.6%",
          },
          {
            id: "widget.legend.titleVisible",
            value: false,
          },
        ],
        localFilters: [],
        showTitle: true,
      },
      model0000017fff2f930d_00000000: {
        type: "live",
        id: "model0000017fff2f930d_00000000",
        visId: "JQGrid",
        titleHtml: {
          translationTable: {
            Default:
              '<div class="widgetTitle" title="" aria-labelledby="model0000017fff2f930d_00000000Title" appcues-data-id="widgetTitle"><div class="textArea"><div class="summernote" style="display: none;"></div><div class="note-editor"><div class="note-dropzone">  <div class="note-dropzone-message"></div></div><div class="note-editing-area"><div class="note-handle"><div class="note-control-selection"><div class="note-control-selection-bg"></div><div class="note-control-holder note-control-nw"></div><div class="note-control-holder note-control-ne"></div><div class="note-control-holder note-control-sw"></div><div class="note-control-sizing note-control-se"></div><div class="note-control-selection-info"></div></div></div><div class="note-editable" contenteditable="true" id="model0000017fff2f930d_00000000Title"><span class="textFitted responsiveColor responsiveFontSize responsiveFontFamily"><p><span>Enter your title here</span></p></span></div></div></div></div></div>',
          },
        },
        name: {
          translationTable: {
            Default: "Enter your title here",
          },
        },
        data: {
          dataViews: [
            {
              modelRef: "model0000017fff1217cd_00000002",
              dataItems: [
                {
                  id: "id1650511713",
                  itemId: `${pairWiseClassErrorsTable}.TRUECLASS`,
                  itemLabel: "True Class",
                  navigationPathId: null,
                },
                {
                  id: "id_1585230647",
                  itemId: `${pairWiseClassErrorsTable}.PREDICTEDCLASS`,
                  itemLabel: "Predicted Class",
                  navigationPathId: null,
                },
                {
                  id: "id1536647827",
                  itemId: `${pairWiseClassErrorsTable}.CONFIDENCE`,
                  itemLabel: "Confidence",
                  aggregate: "avg",
                  format: {
                    formatSpec: {
                      local: true,
                      type: "number",
                      timezone: "America/New_York",
                      locale: "en",
                      maximumFractionDigits: 10,
                      minimumFractionDigits: 10,
                      useGrouping: true,
                    },
                  },
                  navigationPathId: null,
                },
                {
                  id: "id1549813825",
                  itemId: `${pairWiseClassErrorsTable}.PREDICTEDCLASS`,
                  itemLabel: "Predicted Class",
                  aggregate: "count",
                  format: {
                    formatSpec: {
                      local: true,
                      type: "text",
                      timezone: "America/New_York",
                      locale: "en",
                    },
                  },
                  navigationPathId: null,
                },
              ],
              id: "model0000017fff322765_00000000",
              grid: {
                viewOption: "projectedAggregated",
              },
            },
          ],
        },
        visTypeLocked: true,
        slotmapping: {
          slots: [
            {
              name: "grid_cols",
              dataItems: [
                "id1650511713",
                "id_1585230647",
                "id1549813825",
                "id1536647827",
              ],
              dataItemSettings: [],
              layerId: "data",
            },
          ],
        },
        properties: [
          {
            id: "hideSummaries",
            value: false,
          },
          {
            id: "columnHeadingFontBold",
            value: true,
          },
        ],
        localFilters: [],
        queryRefresh: {
          autoRefresh: false,
          unit: "seconds",
          value: "60",
        },
      },
      model0000017fff37764e_00000000: {
        type: "live",
        id: "model0000017fff37764e_00000000",
        visId: "com.ibm.vis.rave2line",
        titleHtml: {
          translationTable: {
            Default:
              '<div class="widgetTitle" title="" aria-labelledby="model0000017fff37764e_00000000Title" appcues-data-id="widgetTitle"><div class="textArea"><div class="summernote" style="display: none;"></div><div class="note-editor"><div class="note-dropzone">  <div class="note-dropzone-message"></div></div><div class="note-editing-area"><div class="note-handle"><div class="note-control-selection" style="display: none;"><div class="note-control-selection-bg"></div><div class="note-control-holder note-control-nw"></div><div class="note-control-holder note-control-ne"></div><div class="note-control-holder note-control-sw"></div><div class="note-control-sizing note-control-se"></div><div class="note-control-selection-info"></div></div></div><div class="note-editable" contenteditable="true" id="model0000017fff37764e_00000000Title"><span class="textFitted responsiveColor responsiveFontSize responsiveFontFamily"><p>Acurácia e Cobertura de acordo com o Confidence Threshold</p></span></div></div></div></div></div>',
          },
        },
        name: {
          translationTable: {
            Default:
              "Acurácia e Cobertura de acordo com o Confidence Threshold",
          },
        },
        data: {
          dataViews: [
            {
              modelRef: "model0000017fff1217cd_00000005",
              dataItems: [
                {
                  id: "id2054118281",
                  itemId: `${accuracyVsCoverageTable}.ACCURACY`,
                  itemLabel: "Accuracy",
                  navigationPathId: null,
                },
                {
                  id: "id_460908424",
                  itemId: `${accuracyVsCoverageTable}.COVERAGE`,
                  itemLabel: "Coverage",
                  navigationPathId: null,
                },
                {
                  id: "_multiMeasuresSeries",
                  itemId: "_multiMeasuresSeries",
                  itemLabel: "Measures group (2)",
                },
                {
                  id: "id_867777022",
                  itemId: `${accuracyVsCoverageTable}.CONFIDENCETHRESHOLD`,
                  itemLabel: "Confidence Threshold",
                  navigationPathId: null,
                },
              ],
              id: "model0000017fff383f1e_00000000",
            },
          ],
        },
        visTypeLocked: true,
        slotmapping: {
          slots: [
            {
              name: "series",
              dataItems: ["_multiMeasuresSeries"],
              dataItemSettings: [],
            },
            {
              name: "values",
              dataItems: ["id_460908424", "id2054118281"],
              dataItemSettings: [],
              layerId: "data",
            },
            {
              name: "categories",
              dataItems: ["id_867777022"],
              dataItemSettings: [],
              layerId: "data",
            },
          ],
        },
        properties: [
          {
            id: "colorPalette",
            value: "colorPalette4",
          },
        ],
        localFilters: [],
        showTitle: true,
      },
      model0000017fff3ebe85_00000002: {
        type: "live",
        id: "model0000017fff3ebe85_00000002",
        visId: "JQGrid",
        titleHtml: {
          translationTable: {
            Default:
              '<div class="widgetTitle" title="" aria-labelledby="model0000017fff2f930d_00000000Title" appcues-data-id="widgetTitle"><div class="textArea"><div class="summernote" style="display: none;"></div><div class="note-editor"><div class="note-dropzone">  <div class="note-dropzone-message"></div></div><div class="note-editing-area"><div class="note-handle"><div class="note-control-selection"><div class="note-control-selection-bg"></div><div class="note-control-holder note-control-nw"></div><div class="note-control-holder note-control-ne"></div><div class="note-control-holder note-control-sw"></div><div class="note-control-sizing note-control-se"></div><div class="note-control-selection-info"></div></div></div><div class="note-editable" contenteditable="true" id="model0000017fff2f930d_00000000Title"><span class="textFitted responsiveColor responsiveFontSize responsiveFontFamily"><p><span>Enter your title here</span></p></span></div></div></div></div></div>',
          },
        },
        name: {
          translationTable: {
            Default: "Enter your title here",
          },
        },
        data: {
          dataViews: [
            {
              modelRef: "model0000017fff1217cd_00000002",
              dataItems: [
                {
                  id: "id_1585230647",
                  itemId: `${pairWiseClassErrorsTable}.PREDICTEDCLASS`,
                  itemLabel: "Predicted Class",
                  navigationPathId: null,
                },
                {
                  id: "id1536647827",
                  itemId: `${pairWiseClassErrorsTable}.CONFIDENCE`,
                  itemLabel: "Confidence",
                  aggregate: "avg",
                  format: {
                    formatSpec: {
                      local: true,
                      type: "number",
                      timezone: "America/New_York",
                      locale: "en",
                      maximumFractionDigits: 10,
                      minimumFractionDigits: 10,
                      useGrouping: true,
                    },
                  },
                  navigationPathId: null,
                },
                {
                  id: "id_1508238722",
                  itemId: `${pairWiseClassErrorsTable}.INPUT_`,
                  itemLabel: "Input",
                  navigationPathId: null,
                },
                {
                  id: "id1390261713",
                  itemId: `${pairWiseClassErrorsTable}.TRUECLASS`,
                  itemLabel: "True Class",
                  navigationPathId: null,
                },
                {
                  id: "id_542359894",
                  itemId: `${pairWiseClassErrorsTable}.CONFIDENCE`,
                  itemLabel: "Confidence",
                  navigationPathId: null,
                },
              ],
              id: "model0000017fff322765_00000000",
              grid: {
                viewOption: "projectedAggregated",
              },
            },
          ],
        },
        visTypeLocked: true,
        slotmapping: {
          slots: [
            {
              name: "grid_cols",
              dataItems: [
                "id_1508238722",
                "id1390261713",
                "id_1585230647",
                "id1536647827",
              ],
              dataItemSettings: [],
              layerId: "data",
            },
            {
              name: "heat",
              dataItems: ["id_542359894"],
              dataItemSettings: [],
              layerId: "data",
            },
          ],
        },
        properties: [
          {
            id: "condColorPalette",
            value: "condColorPalette12",
          },
          {
            id: "hideSummaries",
            value: true,
          },
          {
            id: "columnHeadingFontBold",
            value: true,
          },
        ],
        localFilters: [],
        queryRefresh: {
          autoRefresh: false,
          unit: "seconds",
          value: "60",
        },
        conditions: {
          palette: {
            colors: [
              {
                value: 0,
              },
              {
                value: 0.2,
              },
              {
                value: 0.4,
              },
              {
                value: 0.6000000000000001,
              },
              {
                value: 0.8,
              },
              {
                value: 1,
              },
            ],
          },
        },
      },
      model0000017fff447a44_00000000: {
        type: "live",
        id: "model0000017fff447a44_00000000",
        visId: "JQGrid",
        name: {
          translationTable: {},
        },
        data: {
          dataViews: [
            {
              modelRef: "model0000017fff1217cc_00000002",
              dataItems: [
                {
                  id: "id1448369390",
                  itemId: `${classDistributionTable}.INTENT`,
                  itemLabel: "Intent",
                },
                {
                  id: "id1277556642",
                  itemId: `${classDistributionTable}.COUNT_`,
                  itemLabel: "Count",
                  navigationPathId: null,
                },
              ],
              id: "model0000017fff454964_00000000",
              grid: {
                viewOption: "projectedAggregated",
              },
            },
          ],
        },
        visTypeLocked: true,
        slotmapping: {
          slots: [
            {
              name: "grid_cols",
              dataItems: ["id1448369390", "id1277556642"],
              dataItemSettings: [],
              layerId: "data",
            },
          ],
        },
        properties: [
          {
            id: "columnHeadingFontBold",
            value: true,
          },
        ],
        localFilters: [],
      },
      model0000017fff45f3f8_00000000: {
        type: "live",
        id: "model0000017fff45f3f8_00000000",
        visId: "JQGrid",
        name: {
          translationTable: {},
        },
        data: {
          dataViews: [
            {
              modelRef: "model0000017fff1217cc_00000005",
              dataItems: [
                {
                  id: "id1010568375",
                  itemId: `${precisionAtKTable}.K`,
                  itemLabel: "K",
                  navigationPathId: null,
                },
                {
                  id: "id1973712085",
                  itemId: `${precisionAtKTable}.PRECISION_`,
                  itemLabel: "Precision",
                  format: {
                    formatSpec: {
                      local: true,
                      type: "number",
                      timezone: "America/New_York",
                      locale: "en",
                      maximumFractionDigits: 10,
                      minimumFractionDigits: 10,
                      useGrouping: true,
                    },
                  },
                  navigationPathId: null,
                },
              ],
              id: "model0000017fff46a108_00000000",
              grid: {
                viewOption: "projectedAggregated",
              },
            },
          ],
        },
        visTypeLocked: true,
        slotmapping: {
          slots: [
            {
              name: "grid_cols",
              dataItems: ["id1010568375", "id1973712085"],
              dataItemSettings: [],
              layerId: "data",
            },
          ],
        },
        properties: [
          {
            id: "hideSummaries",
            value: true,
          },
          {
            id: "columnHeadingFontBold",
            value: true,
          },
        ],
        localFilters: [],
      },
      model0000017fff48ec9b_00000000: {
        type: "live",
        id: "model0000017fff48ec9b_00000000",
        visId: "JQGrid",
        name: {
          translationTable: {},
        },
        data: {
          dataViews: [
            {
              modelRef: "model0000017fff1217cc_00000008",
              dataItems: [
                {
                  id: "id1668976708",
                  itemId: `${classAccuracyTable}.CLASS_`,
                  itemLabel: "Class",
                  navigationPathId: null,
                },
                {
                  id: "id1672338317",
                  itemId: `${classAccuracyTable}.COUNT_`,
                  itemLabel: "Count",
                  selection: [
                    {
                      operation: "order",
                      sort: {
                        type: "desc",
                        priority: 0,
                        by: "caption",
                        custom: {},
                      },
                    },
                  ],
                  navigationPathId: null,
                },
                {
                  id: "id676329342",
                  itemId: `${classAccuracyTable}.PRECISION_`,
                  itemLabel: "Precision",
                  aggregate: "avg",
                  navigationPathId: null,
                },
              ],
              id: "model0000017fff497b9e_00000000",
              grid: {
                viewOption: "projectedAggregated",
              },
            },
          ],
        },
        visTypeLocked: true,
        slotmapping: {
          slots: [
            {
              name: "grid_cols",
              dataItems: ["id1668976708", "id1672338317", "id676329342"],
              dataItemSettings: [],
              layerId: "data",
            },
          ],
        },
        properties: [
          {
            id: "columnHeadingFontBold",
            value: true,
          },
        ],
        localFilters: [],
        conditions: {
          palette: {
            colors: [
              {
                value: 0,
              },
              {
                value: 0.2,
              },
              {
                value: 0.4,
              },
              {
                value: 0.6000000000000001,
              },
              {
                value: 0.8,
              },
              {
                value: 1,
              },
            ],
          },
        },
      },
      model0000017fff5cd317_00000000: {
        type: "live",
        id: "model0000017fff5cd317_00000000",
        visId: "com.ibm.vis.ravescatter",
        titleHtml: {
          translationTable: {
            Default:
              '<div class="widgetTitle" title="" aria-labelledby="model0000017fff5cd317_00000000Title" appcues-data-id="widgetTitle"><div class="textArea"><div class="summernote" style="display: none;"></div><div class="note-editor"><div class="note-dropzone">  <div class="note-dropzone-message"></div></div><div class="note-editing-area"><div class="note-handle"><div class="note-control-selection" style="display: none;"><div class="note-control-selection-bg"></div><div class="note-control-holder note-control-nw"></div><div class="note-control-holder note-control-ne"></div><div class="note-control-holder note-control-sw"></div><div class="note-control-sizing note-control-se"></div><div class="note-control-selection-info"></div></div></div><div class="note-editable" contenteditable="false" id="model0000017fff5cd317_00000000Title"><span class="textFitted responsiveColor responsiveFontSize responsiveFontFamily"><p>Nota média vs. Ocorrências</p></span></div></div></div></div></div>',
          },
        },
        name: {
          translationTable: {
            Default: "Nota média vs. Ocorrências",
          },
        },
        data: {
          dataViews: [
            {
              modelRef: "model0000017e7cdcde38_00000002",
              dataItems: [
                {
                  id: "id279678395",
                  itemId: `${logsTable}.NODETITLE`,
                  itemLabel: "Node Title",
                  navigationPathId: null,
                },
                {
                  id: "id1313768377",
                  itemId: `${logsTable}.CONVERSATIONID`,
                  itemLabel: "ID Conversation",
                  aggregate: "countdistinct",
                  navigationPathId: null,
                },
                {
                  id: "id_456014350",
                  itemId: `${logsTable}.C_Curator_Score`,
                  itemLabel: "C_Curator Score",
                  navigationPathId: null,
                },
                {
                  id: "id_1929785377",
                  itemId: `${logsTable}.NODETITLE`,
                  itemLabel: "Node Title",
                  navigationPathId: null,
                },
              ],
              id: "model0000017fff5cff4c_00000000",
            },
          ],
        },
        visTypeLocked: true,
        slotmapping: {
          slots: [
            {
              name: "categories",
              dataItems: ["id279678395"],
              layerId: "data",
            },
            {
              name: "x",
              dataItems: ["id1313768377"],
              dataItemSettings: [],
              layerId: "data",
            },
            {
              name: "y",
              dataItems: ["id_456014350"],
              dataItemSettings: [],
              layerId: "data",
            },
            {
              name: "color",
              dataItems: ["id_1929785377"],
              layerId: "data",
            },
          ],
        },
        properties: [
          {
            id: "itemAxis.title",
            value: {
              translationTable: {
                Default: "Qtd. Ocorrências",
              },
            },
          },
          {
            id: "valueAxis.title",
            value: {
              translationTable: {
                Default: "Nota média",
              },
            },
          },
          {
            id: "widget.legend.size",
            value: "21%",
          },
          {
            id: "widget.legend.titleVisible",
            value: false,
          },
        ],
        localFilters: [],
        showTitle: true,
      },
      model0000017fff5e4fe8_00000000: {
        type: "live",
        id: "model0000017fff5e4fe8_00000000",
        visId: "com.ibm.vis.rave2heat",
        titleHtml: {
          translationTable: {
            Default:
              '<div class="widgetTitle" title="" aria-labelledby="model0000017fff5e4fe8_00000000Title" appcues-data-id="widgetTitle"><div class="textArea"><div class="summernote" style="display: none;"></div><div class="note-editor"><div class="note-dropzone">  <div class="note-dropzone-message"></div></div><div class="note-editing-area"><div class="note-handle"><div class="note-control-selection" style="display: none;"><div class="note-control-selection-bg"></div><div class="note-control-holder note-control-nw"></div><div class="note-control-holder note-control-ne"></div><div class="note-control-holder note-control-sw"></div><div class="note-control-sizing note-control-se"></div><div class="note-control-selection-info"></div></div></div><div class="note-editable" contenteditable="true" id="model0000017fff5e4fe8_00000000Title"><span class="textFitted responsiveColor responsiveFontSize responsiveFontFamily"><p><span>Proporção de Nota por Nó de Diálogo</span></p></span></div></div></div></div></div>',
          },
        },
        name: {
          translationTable: {
            Default: "Proporção de Nota por Nó de Diálogo",
          },
        },
        data: {
          dataViews: [
            {
              modelRef: "model0000017e7cdcde38_00000002",
              dataItems: [
                {
                  id: "id279678395",
                  itemId: `${logsTable}.NODETITLE`,
                  itemLabel: "Node Title",
                  navigationPathId: null,
                },
                {
                  id: "id_456014350",
                  itemId: `${logsTable}.C_Curator_Score`,
                  itemLabel: "C_Curator Score",
                  navigationPathId: null,
                },
                {
                  id: "id1505327620",
                  itemId: `${logsTable}.LOGID`,
                  itemLabel: "ID Log",
                  navigationPathId: null,
                },
              ],
              id: "model0000017fff5ed7be_00000001",
            },
          ],
        },
        visTypeLocked: true,
        slotmapping: {
          slots: [
            {
              name: "categories",
              dataItems: ["id279678395"],
              layerId: "data",
            },
            {
              name: "series",
              dataItems: ["id_456014350"],
              dataItemSettings: [],
              layerId: "data",
            },
            {
              name: "color",
              dataItems: ["id1505327620"],
              layerId: "data",
            },
          ],
        },
        properties: [],
        localFilters: [
          {
            id: `${logsTable}.C_Curator_Score`,
            columnId: `${logsTable}.C_Curator_Score`,
            values: [
              {
                u: null,
                d: null,
                value: null,
                label: null,
                selected: true,
              },
            ],
            excludedValues: [],
            operator: "notin",
            type: null,
            binsLabel: "C_Curator Score",
          },
        ],
        showTitle: true,
      },
      model0000017fff620981_00000000: {
        type: "live",
        id: "model0000017fff620981_00000000",
        visId: "com.ibm.vis.ravescatter",
        titleHtml: {
          translationTable: {
            Default:
              '<div class="widgetTitle" title="" aria-labelledby="model0000017fff620981_00000000Title" appcues-data-id="widgetTitle"><div class="textArea"><div class="summernote" style="display: none;"></div><div class="note-editor"><div class="note-dropzone">  <div class="note-dropzone-message"></div></div><div class="note-editing-area"><div class="note-handle"><div class="note-control-selection" style="display: none;"><div class="note-control-selection-bg"></div><div class="note-control-holder note-control-nw"></div><div class="note-control-holder note-control-ne"></div><div class="note-control-holder note-control-sw"></div><div class="note-control-sizing note-control-se"></div><div class="note-control-selection-info"></div></div></div><div class="note-editable" contenteditable="true" id="model0000017fff620981_00000000Title"><span class="textFitted responsiveColor responsiveFontSize responsiveFontFamily"><p><span>Nota m</span>édia vs. Ocorrências</p></span></div></div></div></div></div>',
          },
        },
        name: {
          translationTable: {
            Default: "Nota média vs. Ocorrências",
          },
        },
        data: {
          dataViews: [
            {
              modelRef: "model0000017e7cdcde38_00000002",
              dataItems: [
                {
                  id: "id1313768377",
                  itemId: `${logsTable}.CONVERSATIONID`,
                  itemLabel: "ID Conversation",
                  aggregate: "countdistinct",
                  navigationPathId: null,
                },
                {
                  id: "id_456014350",
                  itemId: `${logsTable}.C_Curator_Score`,
                  itemLabel: "C_Curator Score",
                  navigationPathId: null,
                },
                {
                  id: "id_52530127",
                  itemId: `${logsTable}.FIRSTINTENT`,
                  itemLabel: "First Intent",
                  navigationPathId: null,
                },
                {
                  id: "id1107705259",
                  itemId: `${logsTable}.FIRSTINTENT`,
                  itemLabel: "First Intent",
                  navigationPathId: null,
                },
              ],
              id: "model0000017fff5cff4c_00000000",
            },
          ],
        },
        visTypeLocked: true,
        slotmapping: {
          slots: [
            {
              name: "categories",
              dataItems: ["id_52530127"],
              dataItemSettings: [],
              layerId: "data",
            },
            {
              name: "x",
              dataItems: ["id1313768377"],
              dataItemSettings: [],
              layerId: "data",
            },
            {
              name: "y",
              dataItems: ["id_456014350"],
              dataItemSettings: [],
              layerId: "data",
            },
            {
              name: "color",
              dataItems: ["id1107705259"],
              dataItemSettings: [],
              layerId: "data",
            },
          ],
        },
        properties: [
          {
            id: "itemAxis.title",
            value: {
              translationTable: {
                Default: "Qtd. Ocorrências",
              },
            },
          },
          {
            id: "valueAxis.title",
            value: {
              translationTable: {
                Default: "Nota média",
              },
            },
          },
          {
            id: "widget.legend.size",
            value: "21%",
          },
          {
            id: "widget.legend.titleVisible",
            value: false,
          },
        ],
        localFilters: [
          {
            id: `${logsTable}.FIRSTINTENT`,
            columnId: `${logsTable}.FIRSTINTENT`,
            values: [
              {
                u: `${logsTable}.FIRSTINTENT->[]`,
                d: "",
                value: `${logsTable}.FIRSTINTENT->[]`,
                label: "",
                selected: true,
              },
            ],
            excludedValues: [],
            operator: "notin",
            type: null,
            binsLabel: "First Intent",
          },
        ],
        showTitle: true,
      },
      model0000017fff62a8d5_00000006: {
        type: "live",
        id: "model0000017fff62a8d5_00000006",
        visId: "com.ibm.vis.rave2heat",
        titleHtml: {
          translationTable: {
            Default:
              '<div class="widgetTitle" title="" aria-labelledby="model0000017fff62a8d5_00000006Title" appcues-data-id="widgetTitle"><div class="textArea"><div class="summernote" style="display: none;"></div><div class="note-editor"><div class="note-dropzone">  <div class="note-dropzone-message"></div></div><div class="note-editing-area"><div class="note-handle"><div class="note-control-selection" style="display: none;"><div class="note-control-selection-bg"></div><div class="note-control-holder note-control-nw"></div><div class="note-control-holder note-control-ne"></div><div class="note-control-holder note-control-sw"></div><div class="note-control-sizing note-control-se"></div><div class="note-control-selection-info"></div></div></div><div class="note-editable" contenteditable="true" id="model0000017fff62a8d5_00000006Title"><span class="textFitted responsiveColor responsiveFontSize responsiveFontFamily"><p><span>Proporção de Nota por Intenção</span></p></span></div></div></div></div></div>',
          },
        },
        name: {
          translationTable: {
            Default: "Proporção de Nota por Intenção",
          },
        },
        data: {
          dataViews: [
            {
              modelRef: "model0000017e7cdcde38_00000002",
              dataItems: [
                {
                  id: "id_456014350",
                  itemId: `${logsTable}.C_Curator_Score`,
                  itemLabel: "C_Curator Score",
                  navigationPathId: null,
                },
                {
                  id: "id1505327620",
                  itemId: `${logsTable}.LOGID`,
                  itemLabel: "ID Log",
                  navigationPathId: null,
                },
                {
                  id: "id_52530127",
                  itemId: `${logsTable}.FIRSTINTENT`,
                  itemLabel: "First Intent",
                  navigationPathId: null,
                },
              ],
              id: "model0000017fff5ed7be_00000001",
            },
          ],
        },
        visTypeLocked: true,
        slotmapping: {
          slots: [
            {
              name: "categories",
              dataItems: ["id_52530127"],
              dataItemSettings: [],
              layerId: "data",
            },
            {
              name: "series",
              dataItems: ["id_456014350"],
              dataItemSettings: [],
              layerId: "data",
            },
            {
              name: "color",
              dataItems: ["id1505327620"],
              layerId: "data",
            },
          ],
        },
        properties: [],
        localFilters: [
          {
            id: `${logsTable}.C_Curator_Score`,
            columnId: `${logsTable}.C_Curator_Score`,
            values: [
              {
                u: null,
                d: null,
                value: null,
                label: null,
                selected: true,
              },
            ],
            excludedValues: [],
            operator: "notin",
            type: null,
            binsLabel: "C_Curator Score",
          },
          {
            id: `${logsTable}.FIRSTINTENT`,
            columnId: `${logsTable}.FIRSTINTENT`,
            values: [
              {
                u: `${logsTable}.FIRSTINTENT->[]`,
                d: "",
                value: `${logsTable}.FIRSTINTENT->[]`,
                label: "",
                selected: true,
              },
            ],
            excludedValues: [],
            operator: "notin",
            type: null,
            binsLabel: "First Intent",
          },
        ],
        showTitle: true,
      },
      model0000017fff6e7a2d_00000000: {
        type: "live",
        id: "model0000017fff6e7a2d_00000000",
        visId: "com.ibm.vis.rave2network",
        name: {
          translationTable: {},
        },
        data: {
          dataViews: [
            {
              modelRef: "model0000017e7cdcde3a_00000013",
              dataItems: [
                {
                  id: "id_57101700",
                  itemId: `${conversationPathTable}.ORIGINNODE`,
                  itemLabel: "Origin Node",
                  navigationPathId: null,
                },
                {
                  id: "id_645157108",
                  itemId: `${conversationPathTable}.DESTINENODE`,
                  itemLabel: "Destine Node",
                  navigationPathId: null,
                },
                {
                  id: "id1628978",
                  itemId: `${conversationPathTable}.CONVERSATIONID`,
                  itemLabel: "Conversation ID",
                  aggregate: "countdistinct",
                  navigationPathId: null,
                },
              ],
              id: "model0000018005281442_00000000",
              grid: {
                viewOption: "projectedAggregated",
              },
            },
          ],
        },
        visTypeLocked: true,
        slotmapping: {
          slots: [
            {
              name: "categories",
              dataItems: ["id_57101700"],
              dataItemSettings: [],
              layerId: "data",
            },
            {
              name: "series",
              dataItems: ["id_645157108"],
              dataItemSettings: [],
              layerId: "data",
            },
            {
              name: "values",
              dataItems: ["id1628978"],
              layerId: "data",
            },
          ],
        },
        properties: [
          {
            id: "colorPalette",
            value: "colorPalette0",
          },
        ],
        localFilters: [
          {
            id: `${conversationPathTable}.DESTINENODE`,
            columnId: `${conversationPathTable}.DESTINENODE`,
            values: [
              {
                u: `${conversationPathTable}.DESTINENODE->[]`,
                d: "",
                value: `${conversationPathTable}.DESTINENODE->[]`,
                label: "",
                selected: true,
              },
            ],
            excludedValues: [],
            operator: "notin",
            type: null,
            binsLabel: "Destine Node",
          },
        ],
      },
      model00000180055d02b8_00000002: {
        type: "live",
        id: "model00000180055d02b8_00000002",
        visId: "list",
        name: {
          translationTable: {},
        },
        data: {
          dataViews: [
            {
              modelRef: "model0000017e7cdcde3a_00000013",
              dataItems: [
                {
                  id: "id_57101700",
                  itemId: `${conversationPathTable}.ORIGINNODE`,
                  itemLabel: "Origin Node",
                  format: {
                    formatSpec: {
                      local: true,
                      type: "text",
                      timezone: "America/New_York",
                      locale: "en",
                    },
                  },
                  navigationPathId: null,
                },
              ],
              id: "model00000180056da6be_00000000",
            },
          ],
        },
        slotmapping: {
          slots: [
            {
              name: "level1",
              dataItems: ["id_57101700"],
              dataItemSettings: [],
              layerId: "data",
            },
          ],
        },
        properties: [],
        localFilters: [],
      },
      model0000018048468cea_00000002: {
        type: "live",
        id: "model0000018048468cea_00000002",
        visId: "summary",
        titleHtml: {
          translationTable: {
            Default:
              '<div class="widgetTitle" title="" aria-labelledby="model0000018048468cea_00000002Title" appcues-data-id="widgetTitle"><div class="textArea hidden"><div class="summernote" style="display: none;"></div><div class="note-editor"><div class="note-dropzone">  <div class="note-dropzone-message"></div></div><div class="note-editing-area"><div class="note-handle"><div class="note-control-selection" style="display: none;"><div class="note-control-selection-bg"></div><div class="note-control-holder note-control-nw"></div><div class="note-control-holder note-control-ne"></div><div class="note-control-holder note-control-sw"></div><div class="note-control-sizing note-control-se"></div><div class="note-control-selection-info"></div></div></div><div class="note-editable" contenteditable="false" id="model0000018048468cea_00000002Title"><p><br></p></div></div></div></div></div>',
          },
        },
        name: {
          translationTable: {},
        },
        data: {
          dataViews: [
            {
              modelRef: "model0000017e7cdcde3a_00000000",
              dataItems: [
                {
                  id: "id1719230215",
                  itemId: `${callsTable}.C_concludedConversatio`,
                  itemLabel: "%concluded",
                  format: {
                    formatSpec: {
                      type: "percent",
                      timezone: "America/New_York",
                      locale: "en",
                      maximumFractionDigits: 2,
                      minimumFractionDigits: 2,
                      convertPercent: false,
                      useGrouping: true,
                      local: true,
                    },
                  },
                  navigationPathId: null,
                },
              ],
              id: "model0000018048468cea_00000000",
            },
          ],
        },
        slotmapping: {
          slots: [
            {
              name: "actual",
              dataItems: ["id1719230215"],
              dataItemSettings: [],
              layerId: "data",
            },
          ],
        },
        properties: [
          {
            id: "baseValueLabel",
            value: {
              translationTable: {
                Default: "Completas",
              },
            },
          },
        ],
        localFilters: [],
        showTitle: false,
        queryRefresh: {
          autoRefresh: false,
          unit: "seconds",
          value: "60",
        },
      },
      model00000180484a631c_00000000: {
        type: "live",
        id: "model00000180484a631c_00000000",
        visId: "com.ibm.vis.rave2bundlecolumn",
        titleHtml: {
          translationTable: {
            Default:
              '<div class="widgetTitle" title="" aria-labelledby="model00000180484a631c_00000000Title" appcues-data-id="widgetTitle"><div class="textArea"><div class="summernote" style="display: none;"></div><div class="note-editor"><div class="note-dropzone">  <div class="note-dropzone-message"></div></div><div class="note-editing-area"><div class="note-handle"><div class="note-control-selection" style="display: none;"><div class="note-control-selection-bg"></div><div class="note-control-holder note-control-nw"></div><div class="note-control-holder note-control-ne"></div><div class="note-control-holder note-control-sw"></div><div class="note-control-sizing note-control-se"></div><div class="note-control-selection-info"></div></div></div><div class="note-editable" contenteditable="true" id="model00000180484a631c_00000000Title"><span class="textFitted responsiveColor responsiveFontSize responsiveFontFamily"><p>Status da Completude das Chamadas</p></span></div></div></div></div></div>',
          },
        },
        name: {
          translationTable: {
            Default: "Status da Completude das Chamadas",
          },
        },
        data: {
          dataViews: [
            {
              modelRef: "model0000017e7cdcde3a_00000000",
              dataItems: [
                {
                  id: "id_2026850761",
                  itemId: `${callsTable}.CONVERSATIONID`,
                  itemLabel: "ID Conversation",
                  aggregate: "countdistinct",
                  navigationPathId: null,
                },
                {
                  id: "id_196299349",
                  itemId: `${callsTable}.CONCLUDED`,
                  itemLabel: "Finished Conversations",
                  navigationPathId: null,
                },
              ],
              id: "model00000180484bcc9a_00000000",
            },
          ],
        },
        visTypeLocked: true,
        slotmapping: {
          slots: [
            {
              name: "values",
              dataItems: ["id_2026850761"],
              dataItemSettings: [],
              layerId: "data",
            },
            {
              name: "categories",
              dataItems: ["id_196299349"],
              dataItemSettings: [],
              layerId: "data",
            },
          ],
        },
        properties: [
          {
            id: "itemAxis.title.visible",
            value: true,
          },
          {
            id: "itemAxis.title.color",
            value: "color3",
          },
          {
            id: "itemAxis.title",
            value: {
              translationTable: {
                Default: "Status",
              },
            },
          },
          {
            id: "valueAxis.title",
            value: {
              translationTable: {
                Default: "Qtd. Conversas",
              },
            },
          },
          {
            id: "itemAxis.ticks.visible",
            value: false,
          },
          {
            id: "valueAxis.ticks.labels.visible",
            value: true,
          },
          {
            id: "valueAxis.ticks.visible",
            value: false,
          },
        ],
        localFilters: [],
        showTitle: true,
      },
    },
  };

  return standardDashboard;
}

module.exports = { getCognosSession, initializeDashboard };
