import { CAT_FETCH_URL } from "./urls.js";
import axios from "axios";

export async function fetcher(url) {
  let res;
  let interval = 200;
  do {
    res = await axios.get(url, {
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

export function sanitizeCat(cat) {
  if (cat.error_message) {
    return cat;
  }
  return {
    Name: cat.Name === undefined ? null : cat.Name,
    ID: cat.ID === undefined ? null : cat.ID,
    Sex: cat.Sex === undefined ? null : cat.Sex,
    Age: cat.Age === undefined ? null : cat.Age,
    CoverPhoto: cat.CoverPhoto === undefined ? null : cat.CoverPhoto,
    Photos: cat.Photos === undefined ? [] : cat.Photos,
    Videos: cat.Videos === undefined ? [] : cat.Videos,
    Breed: cat.Breed === undefined ? null : cat.Breed,
    Color: cat.Color === undefined ? null : cat.Color,
    Pattern: cat.Pattern === undefined ? null : cat.Pattern,
    Description: cat.Description === undefined ? null : cat.Description,
    Attributes: cat.Attributes === undefined ? [] : cat.Attributes,
    LitterGroupId: cat.LitterGroupId === undefined ? null : cat.LitterGroupId,
    PreviousIds: cat.PreviousIds === undefined ? [] : cat.PreviousIds,
    "Internal-ID": cat["Internal-ID"] === undefined ? [] : cat["Internal-ID"],
  };
}

export function sanitizeCats(cats) {
  return cats.map((cat) => sanitizeCat(cat));
}

export async function returnCats() {
  const dirtyCats = await fetchCats(CAT_FETCH_URL, "publishable");
  const cats = sanitizeCats(dirtyCats);
  return cats;
}

export async function returnCatsInCustody() {
  const dirtyCats = await fetchCats(CAT_FETCH_URL, "in%20custody");
  const cats = await sanitizeCats(dirtyCats);
  return cats;
}

export async function returnCat(catId) {
  let adopted, cat;
  const dirtyCat = await fetcher(`${CAT_FETCH_URL}/${catId}`);
  if (
    dirtyCat.Status === "Healthy In Home" ||
    dirtyCat.Status === "Adoptions: Contract and Payment (Not on Website)" ||
    dirtyCat.Status === "Adoptions: Viewing (Not on Website)" ||
    dirtyCat.Status === "Adoptions: Available (Not on Website)"
  ) {
    adopted = true;
  }
  const cleanCat = sanitizeCat(dirtyCat);
  if (adopted === true) {
    cat = { ...cleanCat, Adopted: true };
  } else {
    cat = cleanCat;
  }
  return cat;
}

export async function returnAdoptedCats() {
  const sixMonthsAgo = Math.floor(Date.now() / 1000) - 15552000;
  const dirtyCats = await fetchCats(
    CAT_FETCH_URL,
    "",
    `${sixMonthsAgo}&sort=updated_at`
  );
  const adoptedDirtyCats = dirtyCats.filter(
    (cat) =>
      cat.Status === "Healthy In Home" ||
      cat.Status === "Adoptions: Contract and Payment (Not on Website)" ||
      cat.Status === "Adoptions: Viewing (Not on Website)" ||
      cat.Status === "Adoptions: Available (Not on Website)"
  );
  const adoptedCats = sanitizeCats(adoptedDirtyCats).map((cat) => {
    return { ...cat, Adopted: true };
  });
  return adoptedCats;
}
