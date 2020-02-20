CREATE TABLE `fantasy_manager` (
  `id` varchar(12) NOT NULL,
  `first_name` varchar(45) NOT NULL,
  `last_name` varchar(45) NOT NULL,
  `email` varchar(128) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `fantasy_team` (
  `team_id` int(11) NOT NULL AUTO_INCREMENT,
  `team_name` varchar(45) NOT NULL,
  `manager_id` varchar(12) NOT NULL,
  PRIMARY KEY (`team_id`),
  UNIQUE KEY `team_name_UNIQUE` (`team_name`),
  KEY `team_to_manager_idx` (`manager_id`),
  CONSTRAINT `team_to_manager` FOREIGN KEY (`manager_id`) REFERENCES `fantasy_manager` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB AUTO_INCREMENT=28 DEFAULT CHARSET=utf8;