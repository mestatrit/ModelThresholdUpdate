CREATE TABLE `domainViewability` (
  `id` bigint(11) unsigned NOT NULL AUTO_INCREMENT,
  `url` varchar(255) NOT NULL DEFAULT '',
  `viewability` int(11) NOT NULL DEFAULT '0',
  `brandSafety` int(11) NOT NULL DEFAULT '0',
  `date` datetime NOT NULL,
  PRIMARY KEY (`id`),
  KEY `url_status` (`url`),
  KEY `date_status` (`date`)
);
