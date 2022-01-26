import { GraphQLClient, gql } from "graphql-request";

const GRAPHQL_BASE_URL =
  "https://throbbing-field-210043.us-west-2.aws.cloud.dgraph.io/graphql";

const graphQLClient = new GraphQLClient(GRAPHQL_BASE_URL, {
  headers: {
    "Dg-Auth": process.env.DGRAPH_PROD_CLIENT,
  },
});

export const batchedQueries = async (data, fn, batchSize, connections) => {
  const errors = [];
  const responses = [];
  const retryReq = [];
  var retries = 0;
  let successes = 0;
  for (let i = 0; i < data.length; i += batchSize * connections) {
    const promises = [];
    for (
      let j = i;
      j < i + batchSize * connections && j < data.length;
      j += batchSize
    ) {
      promises.push(fn(data.slice(j, j + batchSize)));
    }
    if (retryReq.length) {
      const req = retryReq.pop();
      promises.push(graphQLClient.request(req.query, req.variables));
      retries = 0;
    }
    const resp = await Promise.allSettled(promises).catch((error) => {
      errors.push({
        type: "promise",
        content: JSON.stringify(error),
      });
      console.error(error);
    });
    if (resp) {
      for (let element of resp) {
        if (element.status === "fulfilled") {
          successes++;
        } else {
          if (retries < 20) {
            retryReq.push(element.reason.request);
            retries++;
          }
          errors.push({
            type: `gql ${fn.name}`,
            content: JSON.stringify({
              ...element.reason.response.errors,
              retries: retries,
            }),
          });
        }
      }
      responses.push(resp);
    }
  }
  return { successes: successes, responses: responses, errors: errors };
};

export const createCats = (cats) => {
  let requests = gql`
    mutation CreateCats($cats: [AddCatInput!]!) {
      addCat(input: $cats, upsert: true) {
        numUids
      }
    }
  `;
  return graphQLClient.request(requests, { cats });
};

export const updateCats = (cats) => {
  let requests = gql`
    mutation UpdateCats($cats: UpdateCatInput!) {
      updateCat(input: $cats) {
        numUids
      }
    }
  `;
  return graphQLClient.request(requests, { cats });
};

export const deleteCats = (cats) => {
  let query = gql`
    mutation DeleteCats($ids: [ID!]) {
      deleteCat(filter: { InternalID: $ids }) {
        msg
        numUids
      }
    }
  `;
  return graphQLClient.request(query, { cats });
};

export const getArrayCatIds = (InternalIDs) => {
  let query = gql`
    query GetArrayCatIds($InternalIDs: [String]) {
      queryCat(filter: { InternalID: { in: $InternalIDs } }) {
        Microchips {
          id
        }
        PreviousIds {
          id
        }
        Videos {
          id
        }
      }
    }
  `;
  return graphQLClient.request(query, { InternalIDs });
};

export const getInternalCatIds = (InternalIDs) => {
  const query = gql`
    query GetInternalCatIds($InternalIDs: [String]) {
      queryCat(filter: { InternalID: { in: $InternalIDs } }) {
        id
        InternalID
      }
    }
  `;
  return graphQLClient.request(query, { InternalIDs });
};

export const getCatByInternalId = (InternalID) => {
  const query = gql`
    query GetCatByInternalId($InternalID: String) {
      getCat(InternalID: $InternalID) {
        Name
        Sex
        Status
        Age
        CoverPhoto
        Photos
        Videos {
          VideoId
          EmbedUrl
          YoutubeUrl
          ThumbUrl
        }
        Breed
        Color
        Pattern
        Description
        Attributes {
          AttributeName
          Publish
          InternalID
        }
        LitterGroupId
        PreviousIds {
          IdValue
          IssuingShelter
          Type
        }
        InternalID
      }
    }
  `;
  return graphQLClient.request(query, { InternalID });
};

export const deleteMicrochips = (ids) => {
  let query = gql`
    mutation DeleteMicroChips($ids: [ID!]) {
      deleteMicrochip(filter: { id: $ids }) {
        msg
        numUids
      }
    }
  `;
  return graphQLClient.request(query, { ids });
};

export const deletePreviousIds = (ids) => {
  let query = gql`
    mutation DeletePreviousIds($ids: [ID!]) {
      deletePreviousId(filter: { id: $ids }) {
        msg
        numUids
      }
    }
  `;
  return graphQLClient.request(query, { ids });
};

export const deletePersons = (ids) => {
  let query = gql`
    mutation DeletePersons($ids: [ID!]) {
      deletePerson(filter: { id: $ids }) {
        msg
        numUids
      }
    }
  `;
  return graphQLClient.request(query, { ids });
};

export const deleteVideos = (ids) => {
  let query = gql`
    mutation DeleteVideos($ids: [ID!]) {
      deleteVideo(filter: { id: $ids }) {
        msg
        numUids
      }
    }
  `;
  return graphQLClient.request(query, { ids });
};

export const deleteCatLocation = (InternalIDs) => {
  let query = gql`
    mutation DeleteCatLocation($InternalIDs: [Cat]) {
      updateLocation(
        input: { filter: { id: null }, remove: { Cats: $InternalIDs } }
      ) {
        numUids
      }
    }
  `;
  return graphQLClient.request(query, { InternalIDs });
};

export const deleteCatAdoptionFeeGroup = (InternalIDs) => {
  let query = gql`
    mutation DeleteCatAdoptionFeeGroup($InternalIDs: [Cat]) {
      updateAdoptionFeeGroup(
        input: { filter: { Id: null }, remove: { Cats: $InternalIDs } }
      ) {
        numUids
      }
    }
  `;
  return graphQLClient.request(query, { InternalIDs });
};

export const deleteCatAttributes = (InternalIDs) => {
  let query = gql`
    mutation DeleteCatAttributes($InternalIDs: [Cat]) {
      updateAttribute(
        input: { filter: { InternalID: null }, remove: { Cats: $InternalIDs } }
      ) {
        numUids
      }
    }
  `;
  return graphQLClient.request(query, { InternalIDs });
};

export const addImportEvent = (ImportEvent) => {
  let requests = gql`
    mutation AddImportEvent($ImportEvent: AddImportEventInput!) {
      addImportEvent(input: [$ImportEvent]) {
        importEvent {
          endTime
          endpoint
          errors {
            content
            type
          }
          imports
          removals
          since
          startTime
          successes
        }
      }
    }
  `;
  return graphQLClient.request(requests, { ImportEvent });
};

export const getPublishableAndAdoptedCatsIds = () => {
  const query = gql`
    query GetPublishableAndAdoptedCatsIds {
      queryCat(
        filter: {
          Status: {
            in: [
              "Adoptions: Available (On Website)"
              "Healthy In Home"
              "Adoptions: Contract and Payment (Not on Website)"
              "Adoptions: Viewing (Not on Website)"
              "Adoptions: Available (Not on Website)"
            ]
          }
        }
        order: { desc: LastUpdatedUnixTime }
        first: 1000
      ) {
        InternalID
      }
    }
  `;
  return graphQLClient.request(query);
};

export const getPublishableCats = () => {
  const query = gql`
    query GetPublishableCats {
      queryCat(
        filter: {
          Status: {
            in: [
              "Adoptions: Available (On Website)"
              "Adoptions: Viewing (On Website)"
            ]
          }
        }
      ) {
        Name
        Sex
        Age
        CoverPhoto
        Photos
        Videos {
          VideoId
          EmbedUrl
          YoutubeUrl
          ThumbUrl
        }
        Breed
        Color
        Pattern
        Description
        Attributes {
          AttributeName
          Publish
          InternalID
        }
        LitterGroupId
        PreviousIds {
          IdValue
          IssuingShelter
          Type
        }
        InternalID
      }
    }
  `;
  return graphQLClient.request(query);
};

export const getLatestEventTimestamp = () => {
  const query = gql`
    query GetLatestEventTimestamp {
      queryImportEvent(order: { desc: startTime }, first: 1) {
        startTime
      }
    }
  `;
  return graphQLClient.request(query);
};

export const createPeople = (people) => {
  let requests = gql`
    mutation CreatePeople($people: [AddPersonInput!]!) {
      addPerson(input: $people, upsert: true) {
        numUids
      }
    }
  `;
  return graphQLClient.request(requests, { people });
};

export const getArrayPersonIds = (InternalIDs) => {
  let query = gql`
    query GetArrayPersonIds($InternalIDs: [String]) {
      queryPerson(filter: { InternalID: { in: $InternalIDs } }) {
        PreviousIds {
          id
        }
      }
    }
  `;
  return graphQLClient.request(query, { InternalIDs });
};

export const getInternalPersonIds = (InternalIDs) => {
  const query = gql`
    query GetInternalPersonIds($InternalIDs: [String]) {
      queryPerson(filter: { InternalID: { in: $InternalIDs } }) {
        id
        InternalID
      }
    }
  `;
  return graphQLClient.request(query, { InternalIDs });
};

export const createEvents = (events) => {
  let requests = gql`
    mutation CreateEvents($events: [AddEventInput!]!) {
      addEvent(input: $events, upsert: true) {
        numUids
      }
    }
  `;
  return graphQLClient.request(requests, { events });
};
