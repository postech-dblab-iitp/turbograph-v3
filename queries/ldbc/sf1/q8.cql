MATCH (p:Person {id: 94}), (friend:Person {firstName: 'Jose'})
WHERE NOT p=friend
WITH p, friend
MATCH path = shortestPath((p)-[:KNOWS*]-(friend))
WITH min(path_length(path)) AS distance, friend
ORDER BY distance ASC, friend.lastName ASC, friend.id ASC
LIMIT 20
MATCH (friend)-[:IS_LOCATED_IN]->(friendCity:Place {label: "City"})
OPTIONAL MATCH (friend)-[studyAt:STUDY_AT]->(uni:Organisation {label: "University"})-[:ORG_IS_LOCATED_IN]->(uniCity:Place {label: "City"})
WITH friend, collect(
    CASE uni.name
        WHEN null THEN null
        ELSE list_creation(uni.name, string(studyAt.classYear), uniCity.name)
    END) AS unis, friendCity, distance
OPTIONAL MATCH (friend)-[workAt:WORK_AT]->(company:Organisation {label: "Company"})-[:ORG_IS_LOCATED_IN]->(companyCountry:Place {label: "Country"})
WITH friend, collect(
    CASE company.name
        WHEN null THEN null
        ELSE list_creation(company.name, string(workAt.workFrom), companyCountry.name)
    END ) AS companies, unis, friendCity, distance
RETURN
    friend.id AS friendId,
    friend.lastName AS friendLastName,
    distance AS distanceFromPerson,
    friend.birthday AS friendBirthday,
    friend.creationDate AS friendCreationDate,
    friend.gender AS friendGender,
    friend.browserUsed AS friendBrowserUsed,
    friend.locationIP AS friendLocationIp,
    friend.email AS friendEmails,
    friend.speaks AS friendLanguages,
    friendCity.name AS friendCityName,
    unis AS friendUniversities,
    companies AS friendCompanies
ORDER BY
    distanceFromPerson ASC,
    friendLastName ASC,
    friendId ASC
LIMIT 20;