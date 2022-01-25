import { fetchCats } from "./api.js";
import {
  createCats,
  getInternalCatIds,
  getArrayCatIds,
  deleteMicrochips,
  deletePreviousIds,
  deleteVideos,
  deleteCatLocation,
  deleteCatAdoptionFeeGroup,
  deleteCatAttributes,
  batchedQueries,
  addImportEvent,
} from "./dgraph.js";

import { CAT_FETCH_URL } from "./urls.js";

const since = 948860151;

export const importCats = async () => {
  try {
    console.log("importCats started");
    const startTime = Math.floor(Date.now() / 1000);
    console.time("Shelterluv fetch");
    const cats = await fetchCats(CAT_FETCH_URL, "", since);
    console.log("Fetch complete - Parsing response");
    const internalIds = cats.map((element) => element["Internal-ID"]);
    // Cat object refactored to conform to graph schema
    for (let cat of cats) {
      delete Object.assign(cat, { ["InternalID"]: cat["Internal-ID"] })[
        "Internal-ID"
      ];
      delete Object.assign(cat, { ["OrgId"]: cat["ID"] })["ID"];
      if (cat.Attributes) {
        const fixedAttributes = cat.Attributes.map((element) => {
          return {
            InternalID: element["Internal-ID"],
            AttributeName: element.AttributeName,
            Publish: element.Publish,
          };
        });
        cat.Attributes = fixedAttributes;
      }
      if (cat.CurrentLocation === null) {
        delete cat.CurrentLocation;
      } else {
        if (cat.CurrentLocation) {
          cat.CurrentLocation.id = cat.CurrentLocation.Tier1;
          if (cat.CurrentLocation.Tier2) {
            cat.CurrentLocation.id = `${cat.CurrentLocation.id} - ${cat.CurrentLocation.Tier2}`;
          }
        }
      }
      delete cat.AssociatedPerson;
      cat.LastIntakeUnixTime = parseInt(cat.LastIntakeUnixTime);
      cat.LastUpdatedUnixTime = parseInt(cat.LastUpdatedUnixTime);
      if (cat.Microchips) {
        cat.Microchips = cat.Microchips.map((element) => {
          return {
            Id: element.Id,
            Issuer: element.Issuer,
            ImplantUnixTime: parseInt(element.ImplantUnixTime),
          };
        });
      }
      if (cat.AdoptionFeeGroup) {
        cat.AdoptionFeeGroup = {
          Id: cat.AdoptionFeeGroup.Id,
          Name: cat.AdoptionFeeGroup.Name,
          Price: parseInt(cat.AdoptionFeeGroup.Price),
          Discount: cat.AdoptionFeeGroup.Discount,
          Tax: cat.AdoptionFeeGroup.Tax,
        };
      }
    }

    console.timeEnd("Shelterluv fetch");
    console.log("Cats parsed - Beginning database update");
    console.time("getInternalCatIds");
    const foundResp = await getInternalCatIds([""].concat(internalIds)).catch(
      (error) => console.error(error)
    );
    console.timeEnd("getInternalCatIds");
    console.time("foundResp.queryCat");

    let found = [];

    if (foundResp.queryCat) {
      found = foundResp.queryCat.map((element) => element.InternalID);
    }
    console.timeEnd("foundResp.queryCat");
    console.time("getArrayCatIds");
    const arrayIds = await getArrayCatIds([""].concat(found)).catch((error) =>
      console.error(error)
    );
    console.timeEnd("getArrayCatIds");

    // Find and remove one-to-one nodes from [Cat]
    const previousIds = [];
    const microchips = [];
    const videos = [];

    for (let i = 0; i < arrayIds.queryCat.length; i++) {
      const previousIdsIds = arrayIds.queryCat[i].PreviousIds.map(
        (element) => element.id
      );
      const microchipIds = arrayIds.queryCat[i].Microchips.map(
        (element) => element.id
      );
      const videosIds = arrayIds.queryCat[i].Videos.map(
        (element) => element.id
      );
      previousIds.push(...previousIdsIds);
      microchips.push(...microchipIds);
      videos.push(...videosIds);
    }

    // Remove one-to-one nodes from [Cat]
    console.time("one-to-one");

    const deletePreviousIdsResp = await batchedQueries(
      previousIds,
      deletePreviousIds,
      200,
      40
    );

    const deleteMicrochipsResp = await batchedQueries(
      microchips,
      deleteMicrochips,
      200,
      40
    );

    const videosResp = await batchedQueries(videos, deleteVideos, 200, 40);
    console.timeEnd("one-to-one");

    // Remove one-to-many Cat nodes from Location and AdoptionFeeGroup
    console.time("one-to-many");
    const deleteCatLocationResp = await batchedQueries(
      foundResp,
      deleteCatLocation,
      500,
      10
    );

    const deleteCatAdoptionFeeGroupResp = await batchedQueries(
      foundResp,
      deleteCatAdoptionFeeGroup,
      500,
      10
    );
    console.timeEnd("one-to-many");
    // Remove many-to-many Cat nodes from Attributes
    console.time("many-to-many");
    const deleteCatAttributesResp = await batchedQueries(
      foundResp,
      deleteCatAttributes,
      500,
      10
    );
    console.timeEnd("many-to-many");

    // createCat mutation re-creates relationships from nested objects
    console.time("createCats");
    const createCatsResp = await batchedQueries(cats, createCats, 500, 20);
    console.timeEnd("createCats");
    console.log("Database update complete - writing UpdateEvent");
    const errors = [].concat(
      ...deletePreviousIdsResp.errors,
      ...deleteMicrochipsResp.errors,
      ...videosResp.errors,
      ...deleteCatLocationResp.errors,
      ...deleteCatAdoptionFeeGroupResp.errors,
      ...deleteCatAttributesResp.errors,
      ...createCatsResp.errors
    );

    const successes =
      deletePreviousIdsResp.successes +
      deleteMicrochipsResp.successes +
      videosResp.successes +
      deleteCatLocationResp.successes +
      deleteCatAdoptionFeeGroupResp.successes +
      deleteCatAttributesResp.successes +
      createCatsResp.successes;

    const respEvent = await addImportEvent({
      endTime: Math.floor(Date.now() / 1000),
      endpoint: "api/cats/import",
      errors,
      imports: cats.length,
      since,
      startTime,
      successes,
    });
    console.log("importCats ended");
  } catch (error) {
    console.error(error);
  }
};
