import { fetchEvents } from "./api.js";
import { createEvents, batchedQueries, addImportEvent } from "./dgraph.js";

import { EVENTS_FETCH_URL } from "./urls.js";

export const importEvents = async () => {
  try {
    console.log("importEvents started");
    const startTime = Math.floor(Date.now() / 1000);
    console.time("Shelterluv fetch");
    const eventsRaw = await fetchEvents(EVENTS_FETCH_URL);
    console.log("Fetch complete - Parsing response");

    var previousEvent;

    var events = [];

    for (let event of eventsRaw) {
      for (let record of event.AssociatedRecords) {
        if (record.Type === "Person") {
          event.Person = { InternalID: record.Id };
        }
        if (record.Type === "Animal") {
          event.Cat = { InternalID: record.Id };
        }
      }
      event.Time = parseInt(event.Time);
      delete event.AssociatedRecords;
      event.id = JSON.stringify(event);
      if (previousEvent) {
        if (previousEvent.id !== event.id) {
          events.push(event);
        }
      }
      previousEvent = event;
    }
    console.timeEnd("Shelterluv fetch");
    console.log("Events parsed - Beginning database update");
    // createEvents mutation re-creates relationships from nested objects
    console.time("createEvent");
    const createEventsResp = await batchedQueries(
      events,
      createEvents,
      500,
      20
    );
    console.timeEnd("createEvent");
    console.log("Database update complete - writing UpdateEvent");
    const errors = [].concat(...createEventsResp.errors);

    const successes = createEventsResp.successes;

    const respEvent = await addImportEvent({
      endTime: Math.floor(Date.now() / 1000),
      endpoint: "api/events/import",
      errors,
      imports: events.length,
      startTime,
      successes,
    });
    console.log("importEvents ended");
  } catch (error) {
    console.error(error);
  }
};
