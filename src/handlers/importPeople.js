import { fetchPeople } from "./api.js";
import {
  createPeople,
  getInternalPersonIds,
  getArrayPersonIds,
  deletePreviousIds,
  batchedQueries,
  addImportEvent,
} from "./dgraph.js";

import { PEOPLE_FETCH_URL } from "./urls.js";

export const importPeople = async () => {
  try {
    console.log("importPeople started");
    const startTime = Math.floor(Date.now() / 1000);
    console.time("Shelterluv fetch");
    const peopleRaw = await fetchPeople(PEOPLE_FETCH_URL);
    console.log("Fetch complete - Parsing response");

    // Person object refactored to conform to graph schema

    var previousPerson;

    var people = [];

    for (let person of peopleRaw) {
      delete Object.assign(person, {
        ["InternalID"]: person["Internal-ID"],
      })["Internal-ID"];
      delete Object.assign(person, { ["OrgId"]: person["ID"] })["ID"];
      delete person.Animal_ids;
      if (previousPerson) {
        if (previousPerson.InternalID !== person.InternalID) {
          people.push(person);
        }
      }
      previousPerson = person;
    }
    const internalIds = people.map((element) => element.InternalID);

    console.timeEnd("Shelterluv fetch");
    console.log("People parsed - Beginning database update");
    console.time("getInternalPersonIds");
    const foundResp = await getInternalPersonIds(
      [""].concat(internalIds)
    ).catch((error) => console.error(error));
    console.timeEnd("getInternalPersonIds");
    console.time("foundResp.queryPerson");

    let found = [];

    if (foundResp.queryPerson) {
      found = foundResp.queryPerson.map((element) => element.InternalID);
    }
    console.timeEnd("foundResp.queryPerson");
    console.time("getArrayPersonIds");
    const arrayIds = await getArrayPersonIds([""].concat(found)).catch(
      (error) => console.error(error)
    );
    console.timeEnd("getArrayPersonIds");
    // Find and remove one-to-one nodes from [Person]

    const previousIds = [];

    for (let i = 0; i < arrayIds.queryPerson.length; i++) {
      const previousIdsIds = arrayIds.queryPerson[i].PreviousIds.map(
        (element) => element.id
      );
      previousIds.push(...previousIdsIds);
    }

    // Remove one-to-one nodes from [Person]
    console.time("one-to-one");
    const deletePreviousIdsResp = await batchedQueries(
      previousIds,
      deletePreviousIds,
      200,
      40
    );
    console.timeEnd("one-to-one");

    // createPeople mutation re-creates relationships from nested objects
    console.time("createPeople");
    const createPeopleResp = await batchedQueries(
      people,
      createPeople,
      500,
      20
    );
    console.timeEnd("createPeople");
    console.log("Database update complete - writing UpdateEvent");
    const errors = [].concat(
      ...deletePreviousIdsResp.errors,
      ...createPeopleResp.errors
    );

    const successes =
      deletePreviousIdsResp.successes + createPeopleResp.successes;

    const respEvent = await addImportEvent({
      endTime: Math.floor(Date.now() / 1000),
      endpoint: "api/people/import",
      errors,
      imports: people.length,
      startTime,
      successes,
    });
    console.log("importPeople ended");
  } catch (error) {
    console.error(error);
  }
};
