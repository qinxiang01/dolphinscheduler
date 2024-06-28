ALTER TABLE `t_ds_alert`
    ADD COLUMN `sdh_title` varchar(64) NULL AFTER `title`,
ADD COLUMN `sdh_content` text NULL AFTER `content`;


