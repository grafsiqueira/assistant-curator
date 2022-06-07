import api from "../services/api";

export async function getAccounts() {
  return new Promise((resolve, reject) => {
    api
      .get("/ibmid/accounts")
      .then((res) => {
        resolve(res.data);
      })
      .catch((err) => reject(err));
  });
}

export async function switchAccount(accountId) {
  return new Promise((resolve, reject) => {
    api
      .get(`/ibmid/accounts/switch?account_id=${accountId}`)
      .then((res) => resolve(res.data))
      .catch((err) => reject(err));
  });
}

export async function getResources() {
  return new Promise((resolve, reject) => {
    api
      .get(`/resources`)
      .then((res) => {
        resolve(res.data);
      })
      .catch((err) => reject(err));
  });
}

export async function getResourceKeys(resourceId) {
  return new Promise((resolve, reject) => {
    api
      .get(`/ibmid/resources/${resourceId}/resource_keys`)
      .then((res) => resolve(res.data))
      .catch((err) => reject(err));
  });
}
