CREATE DATABASE IF NOT EXISTS Skunk_HANDLER_ZOO_DB;
USE DATABASE Skunk_HANDLER_ZOO_DB;

CREATE SCHEMA IF NOT EXISTS ZOO_ANALYSIS;
USE SCHEMA ZOO_ANALYSIS;

CREATE OR REPLACE TABLE ZOO_DATA (
    json_data VARIANT
);

INSERT INTO ZOO_DATA
SELECT PARSE_JSON('
{
"zooName": "Cosmic Critters Galactic Zoo",
"location": "Space Station Delta-7, Sector Gamma-9",
"establishedDate": "2077-01-01",
"director": {
    "name": "Zorp Glorbax",
    "species": "Xylosian"
},
"habitats": [
    {
    "id": "H001",
    "name": "Crystal Caves",
    "environmentType": "Subterranean",
    "sizeSqMeters": 1500,
    "safetyRating": 4.5,
    "features": ["Luminescent Flora", "Geothermal Vents", "Echo Chambers"],
    "currentTempCelsius": 15
    },
    {
    "id": "H002",
    "name": "Azure Aquarium",
    "environmentType": "Aquatic",
    "sizeSqMeters": 3000,
    "safetyRating": 4.8,
    "features": ["Coral Reef Simulation", "High-Pressure Zone", "Bioluminescent Plankton"],
    "currentTempCelsius": 22
    },
    {
    "id": "H003",
    "name": "Floating Forest",
    "environmentType": "Zero-G Jungle",
    "sizeSqMeters": 2500,
    "safetyRating": 4.2,
    "features": ["Magnetic Vines", "Floating Islands", "Simulated Rain"],
    "currentTempCelsius": 28
    },
    {
    "id": "H004",
    "name": "Frozen Tundra",
    "environmentType": "Arctic",
    "sizeSqMeters": 1800,
    "safetyRating": 4.0,
    "features": ["Ice Caves", "Simulated Aurora"],
    "currentTempCelsius": -10
    }
],
"creatures": [
    {
    "id": "C001",
    "name": "Gloob",
    "species": "Gelatinoid",
    "originPlanet": "Xylar",
    "diet": "Photosynthesis",
    "temperament": "Docile",
    "habitatId": "H001",
    "acquisitionDate": "2077-01-15",
    "specialAbilities": null,
    "healthStatus": { "lastCheckup": "2077-03-01", "status": "Excellent" }
    },
    {
    "id": "C002",
    "name": "Finblade",
    "species": "Aqua-Predator",
    "originPlanet": "Neptunia Prime",
    "diet": "Carnivore",
    "temperament": "Aggressive",
    "habitatId": "H002",
    "acquisitionDate": "2077-02-01",
    "specialAbilities": ["Sonar Burst", "Camouflage"],
    "healthStatus": { "lastCheckup": "2077-03-10", "status": "Good" }
    },
    {
    "id": "C003",
    "name": "Sky-Wisp",
    "species": "Aether Flyer",
    "originPlanet": "Cirrus V",
    "diet": "Energy Absorption",
    "temperament": "Shy",
    "habitatId": "H003",
    "acquisitionDate": "2077-02-20",
    "specialAbilities": ["Invisibility", "Phase Shift"],
    "healthStatus": { "lastCheckup": "2077-03-15", "status": "Fair" }
    },
    {
    "id": "C004",
    "name": "Krystal Krawler",
    "species": "Silicate Arthropod",
    "originPlanet": "Xylar",
    "diet": "Mineralvore",
    "temperament": "Neutral",
    "habitatId": "H001",
    "acquisitionDate": "2077-03-05",
    "specialAbilities": ["Crystal Armor", "Burrowing"],
    "healthStatus": { "lastCheckup": "2077-03-20", "status": "Excellent" }
    },
    {
    "id": "C005",
    "name": "Ice Strider",
    "species": "Cryo-Mammal",
    "originPlanet": "Cryonia",
    "diet": "Herbivore",
    "temperament": "Docile",
    "habitatId": "H004",
    "acquisitionDate": "2077-03-10",
    "specialAbilities": ["Thermal Resistance", "Ice Skating"],
    "healthStatus": { "lastCheckup": "2077-03-25", "status": "Good"}
    }
],
"staff": [
    {
    "employeeId": "S001",
    "name": "Grunga",
    "role": "Senior Keeper",
    "species": "Gronk",
    "assignedHabitatIds": ["H001", "H004"]
    },
    {
    "employeeId": "S002",
    "name": "Dr. Elara Vance",
    "role": "Chief Veterinarian",
    "species": "Human",
    "assignedHabitatIds": []
    },
    {
    "employeeId": "S003",
    "name": "Bleep-Bloop",
    "role": "Maintenance Droid",
    "species": "Robotic Unit 7",
    "assignedHabitatIds": ["H002", "H003"]
    }
],
"upcomingEvents": [
    {
    "eventId": "E001",
    "name": "Finblade Feeding Frenzy",
    "type": "Feeding Show",
    "scheduledTime": "2077-04-01T14:00:00Z",
    "locationHabitatId": "H002",
    "involvedCreatureIds": ["C002"]
    },
    {
    "eventId": "E002",
    "name": "Sky-Wisp Visibility Demo",
    "type": "Educational",
    "scheduledTime": "2077-04-05T11:00:00Z",
    "locationHabitatId": "H003",
    "involvedCreatureIds": ["C003"]
    }
]
}
');

--1
SELECT 
    json_data:zooName::STRING AS zoo_name,
    json_data:location::STRING AS location
FROM ZOO_DATA;

--2
SELECT 
    json_data:director.name::STRING AS director_name,
    json_data:director.species::STRING AS director_species
FROM ZOO_DATA;

--3
SELECT 
    creature.value:name::STRING AS creature_name,
    creature.value:species::STRING AS creature_species
FROM ZOO_DATA,
    LATERAL FLATTEN(input => json_data:creatures) creature;

--4
SELECT 
    creature.value:name::STRING AS creature_name
FROM ZOO_DATA,
    LATERAL FLATTEN(input => json_data:creatures) creature
WHERE creature.value:originPlanet::STRING = 'Xylar';

--5
SELECT 
    habitat.value:name::STRING AS habitat_name,
    habitat.value:environmentType::STRING AS environment_type,
    habitat.value:sizeSqMeters::NUMBER AS habitat_size
FROM ZOO_DATA,
    LATERAL FLATTEN(input => json_data:habitats) habitat
WHERE habitat.value:sizeSqMeters::NUMBER > 2000;

--6
SELECT 
    creature.value:name::STRING AS creature_name
FROM ZOO_DATA,
    LATERAL FLATTEN(input => json_data:creatures) creature
WHERE creature.value:specialAbilities IS NOT NULL 
    AND ARRAY_CONTAINS('Camouflage'::VARIANT, creature.value:specialAbilities);

--7
SELECT 
    creature.value:name::STRING AS creature_name,
    creature.value:healthStatus.status::STRING AS health_status
FROM ZOO_DATA,
    LATERAL FLATTEN(input => json_data:creatures) creature
WHERE creature.value:healthStatus.status::STRING != 'Excellent';

--8
SELECT 
    staff_member.value:name::STRING AS staff_name,
    staff_member.value:role::STRING AS staff_role
FROM ZOO_DATA,
    LATERAL FLATTEN(input => json_data:staff) staff_member
WHERE ARRAY_CONTAINS('H001'::VARIANT, staff_member.value:assignedHabitatIds);

--9
SELECT 
    creature.value:habitatId::STRING AS habitat_id,
    COUNT(*) AS creature_count
FROM ZOO_DATA,
    LATERAL FLATTEN(input => json_data:creatures) creature
GROUP BY creature.value:habitatId::STRING
ORDER BY habitat_id;

--10
SELECT DISTINCT 
    feature.value::STRING AS unique_feature
FROM ZOO_DATA,
    LATERAL FLATTEN(input => json_data:habitats) habitat,
    LATERAL FLATTEN(input => habitat.value:features) feature
ORDER BY unique_feature;

--11
SELECT 
    event.value:name::STRING AS event_name,
    event.value:type::STRING AS event_type,
    event.value:scheduledTime::TIMESTAMP_NTZ AS scheduled_time
FROM ZOO_DATA,
    LATERAL FLATTEN(input => json_data:upcomingEvents) event
ORDER BY scheduled_time;

--12
SELECT 
    creature.value:name::STRING AS creature_name,
    habitat.value:environmentType::STRING AS environment_type
FROM ZOO_DATA,
    LATERAL FLATTEN(input => json_data:creatures) creature,
    LATERAL FLATTEN(input => json_data:habitats) habitat
WHERE creature.value:habitatId::STRING = habitat.value:id::STRING
ORDER BY creature_name;
