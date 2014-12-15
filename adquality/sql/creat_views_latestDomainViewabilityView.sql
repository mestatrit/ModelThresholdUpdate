AS SELECT
   `mt`.`url` AS `url`,
   `mt`.`viewability` AS `viewability`,max(`mt`.`date`) AS `date`
FROM `domainViewability` `mt` group by `mt`.`url`;

CREATE VIEW `lastestDomainViewabilityView`
AS SELECT
   `t`.`id` AS `id`,
   `t`.`url` AS `url`,
   `t`.`viewability` AS `viewability`,
   `t`.`brandSafety` AS `brandSafety`,
   `t`.`date` AS `date`
FROM (`domainViewability` `t` join `maxDateView` `MaxT` on(((`MaxT`.`url` = `t`.`url`) and (`MaxT`.`date` = `t`.`date`))));
