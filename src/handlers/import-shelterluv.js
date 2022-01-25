import { importCats } from "./importCats.js";
import { importPeople } from "./importPeople.js";
import { importEvents } from "./importEvents.js";

export const scheduledEventImportShelterluvHandler = async (event, context) => {
  try {
    await importCats();
    await importPeople();
    await importEvents();
  } catch (error) {
    console.error(error);
  }
};
