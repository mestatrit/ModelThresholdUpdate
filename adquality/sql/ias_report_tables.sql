CREATE  VIEW `AdQualityAdGroupsView`
AS SELECT
   `adg`.`networkAdgId` AS `adGroupId`,
   `adg`.`name` AS `name`,(((`ap`.`optimizationFlags` & (1 << 24)) > 0) + (2 * ((`ap`.`optimizationFlags` & (1 << 26)) > 0))) AS `flags`
FROM (`RTB`.`RTB_AdGroupProperty` `ap` join `adplatform`.`adgroup` `adg` on((`ap`.`adGroupId` = `adg`.`networkAdgId`))) where (((`ap`.`optimizationFlags` & ((1 << 24) + (1 << 26))) > 0) and (`adg`.`status` = 'ENABLED'));

create TABLE AdQualityByAdGroup (
  `id` bigint(11) unsigned NOT NULL AUTO_INCREMENT,
  `adgId` bigint(20) NOT NULL DEFAULT 0,
  `viewability` float NOT NULL DEFAULT '0',
  `brandSafety` int(11) NOT NULL DEFAULT '0',
  `updateDate` datetime NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `adgId_updateDate` (`adgId`, `updateDate`)
);

create View AdQualityReportView as
SELECT
   `av`.`adGroupId` AS `adGroupId`,
   av.`name` AS `name`,
   av.`flags` as flags,
   ap.`viewability`,ap.`brandSafety`,
   aa.`viewability` as measuredViewability,
   aa.`brandSafety` as measuredBrandSafety
   
FROM AdQualityAdGroupsView av join `RTB`.`RTB_AdGroupProperty` `ap` on (av.`adGroupId` = ap.`adGroupId`) 
join AdQualityByAdGroup aa on (`av`.`adGroupId` = aa.`adgId`) ;
