#!/bin/bash
sudo apt-get update
sudo apt-get install postgresql postgresql-contrib postgis -y
sudo sed -i '$a host    all             all             0.0.0.0/0               md5' /etc/postgresql/9.5/main/pg_hba.conf
sudo sed -i '$a listen_addresses = '\'''*\'''  /etc/postgresql/9.5/main/postgresql.conf

sudo /etc/init.d/postgresql restart

sudo su - postgres <<HERE

psql <<EOF
\x

CREATE DATABASE fortis;
\connect fortis


CREATE USER ___POSTGRESS_USER___ WITH PASSWORD '___POSTGRESS_PW___';
CREATE EXTENSION postgis;

CREATE TABLE public.tiles
(
  tileid text NOT NULL,
  keyword text NOT NULL,
  period text NOT NULL,
  periodtype text NULL,
  perioddate timestamp NULL,
  source text NOT NULL,
  pos_sentiment real,
  mentions integer,
  geog geography(Point,4326),
  zoom integer,
  layertype text,
  layer text NOT NULL,
  neg_sentiment real,
  CONSTRAINT id PRIMARY KEY (tileid, layer, keyword, period, source)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE public.tiles
  OWNER TO "___POSTGRESS_USER___";

-- Index: public."tileKey"

-- DROP INDEX public."tileKey";

CREATE INDEX "tilePoint"
  ON public.tiles
  USING gist
  (geog);

CREATE INDEX "tileSource"
  ON public.tiles
  USING btree
  (source COLLATE pg_catalog."default");

CREATE INDEX "tileKeyword"
  ON public.tiles
  USING btree
  (keyword COLLATE pg_catalog."default");

CREATE INDEX "tileLayer"
  ON public.tiles
  USING btree
  (layer COLLATE pg_catalog."default");

  CREATE INDEX "tileLayerType"
  ON public.tiles
  USING btree
  (layertype COLLATE pg_catalog."default");

CREATE INDEX "tilePeriod"
  ON public.tiles
  USING btree
  (period COLLATE pg_catalog."default");
  
CREATE INDEX "periodType"
  ON public.tiles
  USING btree
  (periodtype COLLATE pg_catalog."default");

CREATE INDEX "periodDate"
  ON public.tiles
  USING btree
  (perioddate)

CREATE INDEX "tileZoom"
  ON public.tiles
  USING btree
  (zoom);

CREATE TABLE public.localities
(
  geonameid bigint NOT NULL,
  originalsource text  NOT NULL DEFAULT 'geoname', 
  name text NOT NULL,
  aciiname text NOT NULL,
  alternatenames text,
  country_iso text NOT NULL,
  geog geography(Point,4326),
  elevation integer,
  feature_class character varying(5) NOT NULL,
  adminid integer NOT NULL,
  region text,
  population integer,
  ar_name text,
  ur_name text,
  CONSTRAINT localities_pkey PRIMARY KEY (geonameid,originalsource) 
)
WITH (
  OIDS=FALSE
);
ALTER TABLE public.localities
  OWNER TO "___POSTGRESS_USER___";

-- Index: public.geoname_countrycode

-- DROP INDEX public.geoname_countrycode;

CREATE INDEX geoname_countrycode
  ON public.localities
  USING btree
  (country_iso COLLATE pg_catalog."default");

CREATE INDEX geoname_name
  ON public.localities
  USING btree
  (name COLLATE pg_catalog."default");

CREATE INDEX "localityGeog"
  ON public.localities
  USING gist
  (geog);

CREATE TABLE public.tilemessages
(
  messageid text NOT NULL,
  source text NOT NULL,
  keywords text[] NOT NULL,
  createdtime timestamp with time zone NOT NULL,
  pos_sentiment real,
  geog geography(MultiPoint,4326),
  neg_sentiment real,
  en_sentence text,
  ar_sentence text,
  ur_sentence text,
  full_text text,
  link text,
  original_sources text,
  title text,
  orig_language text NOT NULL,
  CONSTRAINT message_pk PRIMARY KEY (messageid, source)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE public.tilemessages
  OWNER TO "___POSTGRESS_USER___";

-- Index: public."messageKeywordsPK"

-- DROP INDEX public."messageKeywordsPK";

CREATE INDEX "messageKeywordsPK"
  ON public.tilemessages
  USING btree
  (keywords COLLATE pg_catalog."default", createdtime);

-- Index: public."messageLocation"

-- DROP INDEX public."messageLocation";

CREATE INDEX "messageLocation"
  ON public.tilemessages
  USING gist
  (geog);

-- Index: public.tilemessages_ar_sentence_idx

-- DROP INDEX public.tilemessages_ar_sentence_idx;

CREATE INDEX tilemessages_ar_sentence_idx
  ON public.tilemessages
  USING btree
  (ar_sentence COLLATE pg_catalog."default");

-- Index: public.tilemessages_en_sentence_idx

-- DROP INDEX public.tilemessages_en_sentence_idx;

CREATE INDEX tilemessages_en_sentence_idx
  ON public.tilemessages
  USING btree
  (en_sentence COLLATE pg_catalog."default");

-- Index: public.tilemessages_keywords_idx

-- DROP INDEX public.tilemessages_keywords_idx;

CREATE INDEX tilemessages_keywords_idx
  ON public.tilemessages
  USING gin
  (keywords COLLATE pg_catalog."default");

CREATE INDEX tilemessages_source_idx
  ON public.tilemessages(source COLLATE pg_catalog."default");

\q
EOF
HERE
exit

