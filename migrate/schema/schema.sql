/*M!999999\- enable the sandbox mode */ 
-- MariaDB dump 10.19-12.1.2-MariaDB, for debian-linux-gnu (x86_64)
--
-- Host: localhost    Database: homeassistant
-- ------------------------------------------------------
-- Server version	12.1.2-MariaDB-ubu2404

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*M!100616 SET @OLD_NOTE_VERBOSITY=@@NOTE_VERBOSITY, NOTE_VERBOSITY=0 */;

--
-- Table structure for table `event_data`
--

DROP TABLE IF EXISTS `event_data`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `event_data` (
  `data_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `hash` int(10) unsigned DEFAULT NULL,
  `shared_data` longtext DEFAULT NULL,
  PRIMARY KEY (`data_id`),
  KEY `ix_event_data_hash` (`hash`)
) ENGINE=InnoDB AUTO_INCREMENT=91 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `event_types`
--

DROP TABLE IF EXISTS `event_types`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `event_types` (
  `event_type_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `event_type` varchar(64) DEFAULT NULL,
  PRIMARY KEY (`event_type_id`),
  UNIQUE KEY `ix_event_types_event_type` (`event_type`)
) ENGINE=InnoDB AUTO_INCREMENT=10 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `events`
--

DROP TABLE IF EXISTS `events`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `events` (
  `event_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `event_type` char(0) DEFAULT NULL,
  `event_data` char(0) DEFAULT NULL,
  `origin` char(0) DEFAULT NULL,
  `origin_idx` smallint(6) DEFAULT NULL,
  `time_fired` char(0) DEFAULT NULL,
  `time_fired_ts` double DEFAULT NULL,
  `context_id` char(0) DEFAULT NULL,
  `context_user_id` char(0) DEFAULT NULL,
  `context_parent_id` char(0) DEFAULT NULL,
  `data_id` bigint(20) DEFAULT NULL,
  `context_id_bin` tinyblob DEFAULT NULL,
  `context_user_id_bin` tinyblob DEFAULT NULL,
  `context_parent_id_bin` tinyblob DEFAULT NULL,
  `event_type_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`event_id`),
  KEY `ix_events_context_id_bin` (`context_id_bin`(16)),
  KEY `ix_events_time_fired_ts` (`time_fired_ts`),
  KEY `ix_events_event_type_id_time_fired_ts` (`event_type_id`,`time_fired_ts`),
  KEY `ix_events_data_id` (`data_id`),
  CONSTRAINT `1` FOREIGN KEY (`data_id`) REFERENCES `event_data` (`data_id`),
  CONSTRAINT `2` FOREIGN KEY (`event_type_id`) REFERENCES `event_types` (`event_type_id`)
) ENGINE=InnoDB AUTO_INCREMENT=183 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `migration_changes`
--

DROP TABLE IF EXISTS `migration_changes`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `migration_changes` (
  `migration_id` varchar(255) NOT NULL,
  `version` smallint(6) NOT NULL,
  PRIMARY KEY (`migration_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `recorder_runs`
--

DROP TABLE IF EXISTS `recorder_runs`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `recorder_runs` (
  `run_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `start` datetime(6) NOT NULL,
  `end` datetime(6) DEFAULT NULL,
  `closed_incorrect` tinyint(1) NOT NULL,
  `created` datetime(6) NOT NULL,
  PRIMARY KEY (`run_id`),
  KEY `ix_recorder_runs_start_end` (`start`,`end`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `schema_changes`
--

DROP TABLE IF EXISTS `schema_changes`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `schema_changes` (
  `change_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `schema_version` int(11) DEFAULT NULL,
  `changed` datetime(6) NOT NULL,
  PRIMARY KEY (`change_id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `state_attributes`
--

DROP TABLE IF EXISTS `state_attributes`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `state_attributes` (
  `attributes_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `hash` int(10) unsigned DEFAULT NULL,
  `shared_attrs` longtext DEFAULT NULL,
  PRIMARY KEY (`attributes_id`),
  KEY `ix_state_attributes_hash` (`hash`)
) ENGINE=InnoDB AUTO_INCREMENT=9 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `states`
--

DROP TABLE IF EXISTS `states`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `states` (
  `state_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `entity_id` char(0) DEFAULT NULL,
  `state` varchar(255) DEFAULT NULL,
  `attributes` char(0) DEFAULT NULL,
  `event_id` smallint(6) DEFAULT NULL,
  `last_changed` char(0) DEFAULT NULL,
  `last_changed_ts` double DEFAULT NULL,
  `last_reported_ts` double DEFAULT NULL,
  `last_updated` char(0) DEFAULT NULL,
  `last_updated_ts` double DEFAULT NULL,
  `old_state_id` bigint(20) DEFAULT NULL,
  `attributes_id` bigint(20) DEFAULT NULL,
  `context_id` char(0) DEFAULT NULL,
  `context_user_id` char(0) DEFAULT NULL,
  `context_parent_id` char(0) DEFAULT NULL,
  `origin_idx` smallint(6) DEFAULT NULL,
  `context_id_bin` tinyblob DEFAULT NULL,
  `context_user_id_bin` tinyblob DEFAULT NULL,
  `context_parent_id_bin` tinyblob DEFAULT NULL,
  `metadata_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`state_id`),
  KEY `ix_states_attributes_id` (`attributes_id`),
  KEY `ix_states_last_updated_ts` (`last_updated_ts`),
  KEY `ix_states_context_id_bin` (`context_id_bin`(16)),
  KEY `ix_states_metadata_id_last_updated_ts` (`metadata_id`,`last_updated_ts`),
  KEY `ix_states_old_state_id` (`old_state_id`),
  CONSTRAINT `1` FOREIGN KEY (`old_state_id`) REFERENCES `states` (`state_id`),
  CONSTRAINT `2` FOREIGN KEY (`attributes_id`) REFERENCES `state_attributes` (`attributes_id`),
  CONSTRAINT `3` FOREIGN KEY (`metadata_id`) REFERENCES `states_meta` (`metadata_id`)
) ENGINE=InnoDB AUTO_INCREMENT=18 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `states_meta`
--

DROP TABLE IF EXISTS `states_meta`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `states_meta` (
  `metadata_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `entity_id` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`metadata_id`),
  UNIQUE KEY `ix_states_meta_entity_id` (`entity_id`)
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `statistics`
--

DROP TABLE IF EXISTS `statistics`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `statistics` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `created` char(0) DEFAULT NULL,
  `created_ts` double DEFAULT NULL,
  `metadata_id` bigint(20) DEFAULT NULL,
  `start` char(0) DEFAULT NULL,
  `start_ts` double DEFAULT NULL,
  `mean` double DEFAULT NULL,
  `mean_weight` double DEFAULT NULL,
  `min` double DEFAULT NULL,
  `max` double DEFAULT NULL,
  `last_reset` char(0) DEFAULT NULL,
  `last_reset_ts` double DEFAULT NULL,
  `state` double DEFAULT NULL,
  `sum` double DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `ix_statistics_statistic_id_start_ts` (`metadata_id`,`start_ts`),
  KEY `ix_statistics_start_ts` (`start_ts`),
  CONSTRAINT `1` FOREIGN KEY (`metadata_id`) REFERENCES `statistics_meta` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `statistics_meta`
--

DROP TABLE IF EXISTS `statistics_meta`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `statistics_meta` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `statistic_id` varchar(255) DEFAULT NULL,
  `source` varchar(32) DEFAULT NULL,
  `unit_of_measurement` varchar(255) DEFAULT NULL,
  `unit_class` varchar(255) DEFAULT NULL,
  `has_mean` tinyint(1) DEFAULT NULL,
  `has_sum` tinyint(1) DEFAULT NULL,
  `name` varchar(255) DEFAULT NULL,
  `mean_type` smallint(6) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `ix_statistics_meta_statistic_id` (`statistic_id`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `statistics_runs`
--

DROP TABLE IF EXISTS `statistics_runs`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `statistics_runs` (
  `run_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `start` datetime(6) NOT NULL,
  PRIMARY KEY (`run_id`),
  KEY `ix_statistics_runs_start` (`start`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `statistics_short_term`
--

DROP TABLE IF EXISTS `statistics_short_term`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `statistics_short_term` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `created` char(0) DEFAULT NULL,
  `created_ts` double DEFAULT NULL,
  `metadata_id` bigint(20) DEFAULT NULL,
  `start` char(0) DEFAULT NULL,
  `start_ts` double DEFAULT NULL,
  `mean` double DEFAULT NULL,
  `mean_weight` double DEFAULT NULL,
  `min` double DEFAULT NULL,
  `max` double DEFAULT NULL,
  `last_reset` char(0) DEFAULT NULL,
  `last_reset_ts` double DEFAULT NULL,
  `state` double DEFAULT NULL,
  `sum` double DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `ix_statistics_short_term_statistic_id_start_ts` (`metadata_id`,`start_ts`),
  KEY `ix_statistics_short_term_start_ts` (`start_ts`),
  CONSTRAINT `1` FOREIGN KEY (`metadata_id`) REFERENCES `statistics_meta` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*M!100616 SET NOTE_VERBOSITY=@OLD_NOTE_VERBOSITY */;

-- Dump completed on 2025-12-08 14:28:27

