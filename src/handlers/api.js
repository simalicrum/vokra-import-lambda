import { CAT_FETCH_URL } from "./urls.js";
import axios from "axios";

const customAxios = axios.create();

const responseHandler = (response) => {
  return response;
};

const errorHandler = (error) => {
  if (error.response.status === 429) {
    return error.response;
  }
  return Promise.reject(error);
};

customAxios.interceptors.response.use(
  (response) => responseHandler(response),
  (error) => errorHandler(error)
);

export async function fetcher(url) {
  try {
    let res;
    let interval = 200;
    do {
      res = await customAxios.get(url, {
        method: "GET",
        headers: {
          "X-API-Key": process.env.API_KEY,
        },
      });
      if (res.status === 429) {
        let timeout = await new Promise((resolve) =>
          setTimeout(resolve, interval)
        );
      }
      if (interval < 1600) {
        interval *= 2;
      }
    } while (res.status === 429);
    const resp = res.data;
    return resp;
  } catch (error) {
    console.error(error);
  }
}

export const batchedFetches = async (
  url,
  responseProperty,
  connections,
  first
) => {
  let offset = connections,
    resp = [],
    totalCount,
    promises = [];
  let res = await fetcher(url);
  totalCount = res.total_count;
  resp.push(...res[responseProperty]);

  while (offset < totalCount) {
    promises.push(fetcher(`${url}&offset=${offset}`));
    offset += connections;
  }
  const moreResp = await Promise.all(promises).then((res) => {
    res.forEach((element) => resp.push(...element[responseProperty]));
  });
  return resp;
};

export async function fetchCats(url, status = "", since = "") {
  let cats = [];
  cats = batchedFetches(
    `${url}?sort=updated_at&status_type=${status}&since=${since}`,
    "animals",
    100
  );
  return cats;
}

export async function fetchPeople(url, first) {
  let people = [];
  people = batchedFetches(url, "people", 100, first);
  return people;
}

export async function fetchEvents(url) {
  let events = [];
  events = batchedFetches(url, "events", 100);
  return events;
}
