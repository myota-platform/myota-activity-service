--
-- PostgreSQL database dump
--


-- Dumped from database version 16.15
-- Dumped by pg_dump version 16.15

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: citext; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS citext WITH SCHEMA public;


--
-- Name: EXTENSION citext; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION citext IS 'data type for case-insensitive character strings';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: activity_activation; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.activity_activation (
    id uuid NOT NULL,
    programme_slug text NOT NULL,
    entity_id text NOT NULL,
    entity_type text,
    jurisdiction_code text,
    operator_id text NOT NULL,
    operator_callsign public.citext,
    started_at timestamp with time zone NOT NULL,
    ended_at timestamp with time zone,
    validity_expires_at timestamp with time zone,
    status text DEFAULT 'OPEN'::text NOT NULL,
    location jsonb DEFAULT '{}'::jsonb NOT NULL,
    programme_rules jsonb DEFAULT '{}'::jsonb NOT NULL,
    rule_evaluation jsonb DEFAULT '{}'::jsonb NOT NULL,
    qso_count integer DEFAULT 0 NOT NULL,
    unique_callsign_count integer DEFAULT 0 NOT NULL,
    unique_entity_count integer DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT activity_activation_status_check CHECK ((status = ANY (ARRAY['OPEN'::text, 'CLOSED'::text, 'CLOSED_INVALID'::text, 'CANCELLED'::text])))
);


--
-- Name: activity_asset; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.activity_asset (
    id uuid NOT NULL,
    kind text NOT NULL,
    object_key text NOT NULL,
    bucket text NOT NULL,
    media_type text NOT NULL,
    width_px integer NOT NULL,
    height_px integer NOT NULL,
    content_status text DEFAULT 'MISSING'::text NOT NULL,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT activity_asset_kind_check CHECK ((kind = ANY (ARRAY['BACKGROUND'::text, 'SIGNATURE'::text])))
);


--
-- Name: activity_award_definition; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.activity_award_definition (
    id uuid NOT NULL,
    programme_slug text NOT NULL,
    code text NOT NULL,
    version integer DEFAULT 1 NOT NULL,
    status text DEFAULT 'DRAFT'::text NOT NULL,
    effective_from timestamp with time zone,
    retired_at timestamp with time zone,
    definition jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: activity_award_issuance; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.activity_award_issuance (
    id uuid NOT NULL,
    request_id uuid NOT NULL,
    award_id uuid NOT NULL,
    programme_slug text NOT NULL,
    level_id text NOT NULL,
    subject_id text NOT NULL,
    issuance jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: activity_award_progress; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.activity_award_progress (
    award_id uuid NOT NULL,
    award_version integer NOT NULL,
    subject_id text NOT NULL,
    category text NOT NULL,
    facts jsonb DEFAULT '{}'::jsonb NOT NULL,
    evaluation jsonb DEFAULT '{}'::jsonb NOT NULL,
    computed_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: activity_award_request; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.activity_award_request (
    id uuid NOT NULL,
    award_id uuid NOT NULL,
    programme_slug text NOT NULL,
    level_id text NOT NULL,
    subject_id text NOT NULL,
    category text NOT NULL,
    callsign text NOT NULL,
    person_name text NOT NULL,
    facts jsonb DEFAULT '{}'::jsonb NOT NULL,
    status text DEFAULT 'REQUESTED'::text NOT NULL,
    issued_award_id uuid,
    requested_at timestamp with time zone DEFAULT now() NOT NULL,
    issued_at timestamp with time zone
);


--
-- Name: activity_import; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.activity_import (
    id uuid NOT NULL,
    activation_id uuid NOT NULL,
    filename text NOT NULL,
    object_key text NOT NULL,
    bucket text NOT NULL,
    sha256 text NOT NULL,
    content_size bigint NOT NULL,
    malware_status text DEFAULT 'PENDING'::text NOT NULL,
    status text DEFAULT 'QUEUED'::text NOT NULL,
    records_seen integer DEFAULT 0 NOT NULL,
    records_accepted integer DEFAULT 0 NOT NULL,
    records_rejected integer DEFAULT 0 NOT NULL,
    errors jsonb DEFAULT '[]'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    started_at timestamp with time zone,
    completed_at timestamp with time zone,
    source_deleted_at timestamp with time zone
);


--
-- Name: activity_job; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.activity_job (
    id uuid NOT NULL,
    kind text NOT NULL,
    payload jsonb NOT NULL,
    status text DEFAULT 'QUEUED'::text NOT NULL,
    attempts integer DEFAULT 0 NOT NULL,
    idempotency_key text,
    available_at timestamp with time zone DEFAULT now() NOT NULL,
    started_at timestamp with time zone,
    completed_at timestamp with time zone,
    last_error text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    lease_expires_at timestamp with time zone,
    lease_token uuid,
    CONSTRAINT activity_job_status_check CHECK ((status = ANY (ARRAY['QUEUED'::text, 'RUNNING'::text, 'SUCCEEDED'::text, 'FAILED'::text])))
);


--
-- Name: activity_notification; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.activity_notification (
    id uuid NOT NULL,
    recipient_id text NOT NULL,
    notification_type text NOT NULL,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    status text DEFAULT 'DELIVERED'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    delivered_at timestamp with time zone,
    deduplication_key text
);


--
-- Name: activity_notification_redrive; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.activity_notification_redrive (
    id uuid NOT NULL,
    event_id uuid NOT NULL,
    actor text NOT NULL,
    reason text NOT NULL,
    status text NOT NULL,
    requested_at timestamp with time zone DEFAULT now() NOT NULL,
    published_at timestamp with time zone,
    CONSTRAINT activity_notification_redrive_actor_check CHECK (((length(actor) >= 1) AND (length(actor) <= 120))),
    CONSTRAINT activity_notification_redrive_reason_check CHECK (((length(reason) >= 1) AND (length(reason) <= 500))),
    CONSTRAINT activity_notification_redrive_status_check CHECK ((status = ANY (ARRAY['PENDING'::text, 'PUBLISHED'::text])))
);


--
-- Name: activity_qso; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.activity_qso (
    id uuid NOT NULL,
    activation_id uuid NOT NULL,
    programme_slug text NOT NULL,
    operator_id text NOT NULL,
    operator_callsign public.citext,
    hunter_id text,
    hunter_callsign public.citext,
    worked_callsign public.citext NOT NULL,
    worked_station_key text NOT NULL,
    worked_entity_id text,
    occurred_at timestamp with time zone NOT NULL,
    band text,
    mode text,
    rst text,
    source text DEFAULT 'manual'::text NOT NULL,
    source_import_id uuid,
    deduplication_key text NOT NULL,
    status text DEFAULT 'VALID'::text NOT NULL,
    raw_payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    corrected_at timestamp with time zone,
    CONSTRAINT activity_qso_status_check CHECK ((status = ANY (ARRAY['VALID'::text, 'CORRECTED'::text, 'VOID'::text])))
);


--
-- Name: TABLE activity_qso; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.activity_qso IS 'Service-owned QSOs. Global entity deletion removes linked valid rows, rebuilds aggregates, and queues award recalculation.';


--
-- Name: activity_qso_correction; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.activity_qso_correction (
    id uuid NOT NULL,
    qso_id uuid NOT NULL,
    requested_by text NOT NULL,
    reason text NOT NULL,
    proposed_values jsonb NOT NULL,
    status text DEFAULT 'PENDING'::text NOT NULL,
    reviewed_by text,
    review_note text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    reviewed_at timestamp with time zone,
    CONSTRAINT activity_qso_correction_status_check CHECK ((status = ANY (ARRAY['PENDING'::text, 'APPLIED'::text, 'REJECTED'::text])))
);


--
-- Name: activity_statistic_snapshot; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.activity_statistic_snapshot (
    id uuid NOT NULL,
    programme_slug text NOT NULL,
    statistic_type text NOT NULL,
    scope_key text NOT NULL,
    period_start timestamp with time zone,
    period_end timestamp with time zone,
    metric_values jsonb NOT NULL,
    algorithm_version text NOT NULL,
    generated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: activity_subject_aggregate; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.activity_subject_aggregate (
    programme_slug text NOT NULL,
    subject_id text NOT NULL,
    category text NOT NULL,
    qso_count bigint DEFAULT 0 NOT NULL,
    activation_count bigint DEFAULT 0 NOT NULL,
    unique_callsign_count bigint DEFAULT 0 NOT NULL,
    unique_entity_count bigint DEFAULT 0 NOT NULL,
    last_entity_type text,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT activity_subject_aggregate_category_check CHECK ((category = ANY (ARRAY['ACTIVATOR'::text, 'HUNTER'::text])))
);


--
-- Name: activity_subject_callsign; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.activity_subject_callsign (
    programme_slug text NOT NULL,
    subject_id text NOT NULL,
    category text NOT NULL,
    callsign public.citext NOT NULL
);


--
-- Name: activity_subject_entity; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.activity_subject_entity (
    programme_slug text NOT NULL,
    subject_id text NOT NULL,
    category text NOT NULL,
    entity_id text NOT NULL
);


--
-- Name: activity_work_dead_letter; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.activity_work_dead_letter (
    id uuid NOT NULL,
    work_id uuid,
    work_type text,
    subject text NOT NULL,
    delivery_count integer NOT NULL,
    error_category text NOT NULL,
    dead_lettered_at timestamp with time zone DEFAULT now() NOT NULL,
    resolved_at timestamp with time zone,
    CONSTRAINT activity_work_dead_letter_delivery_count_check CHECK ((delivery_count > 0))
);


--
-- Name: activity_work_redrive_audit; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.activity_work_redrive_audit (
    id uuid NOT NULL,
    job_id uuid NOT NULL,
    outbox_event_id uuid NOT NULL,
    actor text NOT NULL,
    reason text NOT NULL,
    requested_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT activity_work_redrive_audit_actor_check CHECK (((length(actor) >= 1) AND (length(actor) <= 120))),
    CONSTRAINT activity_work_redrive_audit_reason_check CHECK (((length(reason) >= 1) AND (length(reason) <= 500)))
);


--
-- Name: consumer_checkpoint; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.consumer_checkpoint (
    consumer text NOT NULL,
    last_event_id uuid,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: consumer_processed_event; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.consumer_processed_event (
    consumer text NOT NULL,
    event_id uuid NOT NULL,
    processed_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: dead_letter_event; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dead_letter_event (
    event_id uuid NOT NULL,
    event_type text NOT NULL,
    payload jsonb NOT NULL,
    attempts integer NOT NULL,
    error text,
    dead_lettered_at timestamp with time zone DEFAULT now() NOT NULL,
    resolved_at timestamp with time zone,
    stream_name text,
    stream_sequence bigint,
    subject text
);


--
-- Name: idempotency_record; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.idempotency_record (
    service text NOT NULL,
    key text NOT NULL,
    response jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: outbox_event; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.outbox_event (
    event_id uuid NOT NULL,
    event_type text NOT NULL,
    producer text NOT NULL,
    aggregate_type text NOT NULL,
    aggregate_id text NOT NULL,
    payload jsonb NOT NULL,
    occurred_at timestamp with time zone NOT NULL,
    available_at timestamp with time zone DEFAULT now() NOT NULL,
    attempts integer DEFAULT 0 NOT NULL,
    published_at timestamp with time zone,
    last_error text,
    correlation_id text,
    CONSTRAINT outbox_event_correlation_id_length_check CHECK (((correlation_id IS NULL) OR (((length(correlation_id) >= 1) AND (length(correlation_id) <= 128)) AND (correlation_id !~ '[^!-~]'::text))))
);


--
-- Name: outbox_redrive_audit; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.outbox_redrive_audit (
    id bigint NOT NULL,
    event_id uuid NOT NULL,
    actor text NOT NULL,
    reason text NOT NULL,
    previous_attempts integer NOT NULL,
    previous_error text,
    requested_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT outbox_redrive_audit_actor_check CHECK (((length(actor) >= 1) AND (length(actor) <= 120))),
    CONSTRAINT outbox_redrive_audit_reason_check CHECK (((length(reason) >= 1) AND (length(reason) <= 500)))
);


--
-- Name: outbox_redrive_audit_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.outbox_redrive_audit_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: outbox_redrive_audit_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.outbox_redrive_audit_id_seq OWNED BY public.outbox_redrive_audit.id;


--
-- Name: service_state; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.service_state (
    service text NOT NULL,
    state jsonb NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: outbox_redrive_audit id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.outbox_redrive_audit ALTER COLUMN id SET DEFAULT nextval('public.outbox_redrive_audit_id_seq'::regclass);


--
-- Name: activity_activation activity_activation_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.activity_activation
    ADD CONSTRAINT activity_activation_pkey PRIMARY KEY (id);


--
-- Name: activity_asset activity_asset_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.activity_asset
    ADD CONSTRAINT activity_asset_pkey PRIMARY KEY (id);


--
-- Name: activity_award_definition activity_award_definition_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.activity_award_definition
    ADD CONSTRAINT activity_award_definition_pkey PRIMARY KEY (id);


--
-- Name: activity_award_definition activity_award_definition_programme_slug_code_version_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.activity_award_definition
    ADD CONSTRAINT activity_award_definition_programme_slug_code_version_key UNIQUE (programme_slug, code, version);


--
-- Name: activity_award_issuance activity_award_issuance_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.activity_award_issuance
    ADD CONSTRAINT activity_award_issuance_pkey PRIMARY KEY (id);


--
-- Name: activity_award_progress activity_award_progress_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.activity_award_progress
    ADD CONSTRAINT activity_award_progress_pkey PRIMARY KEY (award_id, award_version, subject_id);


--
-- Name: activity_award_request activity_award_request_award_id_level_id_subject_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.activity_award_request
    ADD CONSTRAINT activity_award_request_award_id_level_id_subject_id_key UNIQUE (award_id, level_id, subject_id);


--
-- Name: activity_award_request activity_award_request_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.activity_award_request
    ADD CONSTRAINT activity_award_request_pkey PRIMARY KEY (id);


--
-- Name: activity_import activity_import_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.activity_import
    ADD CONSTRAINT activity_import_pkey PRIMARY KEY (id);


--
-- Name: activity_job activity_job_idempotency_key_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.activity_job
    ADD CONSTRAINT activity_job_idempotency_key_key UNIQUE (idempotency_key);


--
-- Name: activity_job activity_job_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.activity_job
    ADD CONSTRAINT activity_job_pkey PRIMARY KEY (id);


--
-- Name: activity_notification activity_notification_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.activity_notification
    ADD CONSTRAINT activity_notification_pkey PRIMARY KEY (id);


--
-- Name: activity_notification_redrive activity_notification_redrive_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.activity_notification_redrive
    ADD CONSTRAINT activity_notification_redrive_pkey PRIMARY KEY (id);


--
-- Name: activity_qso_correction activity_qso_correction_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.activity_qso_correction
    ADD CONSTRAINT activity_qso_correction_pkey PRIMARY KEY (id);


--
-- Name: activity_qso activity_qso_deduplication_key_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.activity_qso
    ADD CONSTRAINT activity_qso_deduplication_key_key UNIQUE (deduplication_key);


--
-- Name: activity_qso activity_qso_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.activity_qso
    ADD CONSTRAINT activity_qso_pkey PRIMARY KEY (id);


--
-- Name: activity_statistic_snapshot activity_statistic_snapshot_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.activity_statistic_snapshot
    ADD CONSTRAINT activity_statistic_snapshot_pkey PRIMARY KEY (id);


--
-- Name: activity_statistic_snapshot activity_statistic_snapshot_programme_slug_statistic_type_s_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.activity_statistic_snapshot
    ADD CONSTRAINT activity_statistic_snapshot_programme_slug_statistic_type_s_key UNIQUE (programme_slug, statistic_type, scope_key, period_start, period_end, algorithm_version);


--
-- Name: activity_subject_aggregate activity_subject_aggregate_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.activity_subject_aggregate
    ADD CONSTRAINT activity_subject_aggregate_pkey PRIMARY KEY (programme_slug, subject_id, category);


--
-- Name: activity_subject_callsign activity_subject_callsign_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.activity_subject_callsign
    ADD CONSTRAINT activity_subject_callsign_pkey PRIMARY KEY (programme_slug, subject_id, category, callsign);


--
-- Name: activity_subject_entity activity_subject_entity_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.activity_subject_entity
    ADD CONSTRAINT activity_subject_entity_pkey PRIMARY KEY (programme_slug, subject_id, category, entity_id);


--
-- Name: activity_work_dead_letter activity_work_dead_letter_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.activity_work_dead_letter
    ADD CONSTRAINT activity_work_dead_letter_pkey PRIMARY KEY (id);


--
-- Name: activity_work_redrive_audit activity_work_redrive_audit_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.activity_work_redrive_audit
    ADD CONSTRAINT activity_work_redrive_audit_pkey PRIMARY KEY (id);


--
-- Name: consumer_checkpoint consumer_checkpoint_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.consumer_checkpoint
    ADD CONSTRAINT consumer_checkpoint_pkey PRIMARY KEY (consumer);


--
-- Name: consumer_processed_event consumer_processed_event_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.consumer_processed_event
    ADD CONSTRAINT consumer_processed_event_pkey PRIMARY KEY (consumer, event_id);


--
-- Name: dead_letter_event dead_letter_event_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dead_letter_event
    ADD CONSTRAINT dead_letter_event_pkey PRIMARY KEY (event_id);


--
-- Name: idempotency_record idempotency_record_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.idempotency_record
    ADD CONSTRAINT idempotency_record_pkey PRIMARY KEY (service, key);


--
-- Name: outbox_event outbox_event_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.outbox_event
    ADD CONSTRAINT outbox_event_pkey PRIMARY KEY (event_id);


--
-- Name: outbox_redrive_audit outbox_redrive_audit_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.outbox_redrive_audit
    ADD CONSTRAINT outbox_redrive_audit_pkey PRIMARY KEY (id);


--
-- Name: service_state service_state_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.service_state
    ADD CONSTRAINT service_state_pkey PRIMARY KEY (service);


--
-- Name: activity_activation_date_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX activity_activation_date_idx ON public.activity_activation USING btree (started_at DESC);


--
-- Name: activity_activation_entity_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX activity_activation_entity_idx ON public.activity_activation USING btree (entity_id, started_at DESC);


--
-- Name: activity_activation_jurisdiction_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX activity_activation_jurisdiction_idx ON public.activity_activation USING btree (programme_slug, jurisdiction_code, started_at DESC);


--
-- Name: activity_activation_operator_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX activity_activation_operator_idx ON public.activity_activation USING btree (operator_id, started_at DESC);


--
-- Name: activity_activation_programme_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX activity_activation_programme_idx ON public.activity_activation USING btree (programme_slug, status, started_at DESC);


--
-- Name: activity_award_issuance_subject_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX activity_award_issuance_subject_idx ON public.activity_award_issuance USING btree (subject_id, created_at DESC);


--
-- Name: activity_award_public_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX activity_award_public_idx ON public.activity_award_definition USING btree (programme_slug, status, effective_from);


--
-- Name: activity_award_request_subject_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX activity_award_request_subject_idx ON public.activity_award_request USING btree (subject_id, status, requested_at DESC);


--
-- Name: activity_consumer_dead_letter_replay_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX activity_consumer_dead_letter_replay_idx ON public.dead_letter_event USING btree (stream_name, stream_sequence) WHERE ((resolved_at IS NULL) AND (stream_sequence IS NOT NULL));


--
-- Name: activity_import_adif_retention_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX activity_import_adif_retention_idx ON public.activity_import USING btree (completed_at, id) WHERE ((status = ANY (ARRAY['COMPLETED'::text, 'FAILED'::text])) AND (source_deleted_at IS NULL));


--
-- Name: activity_import_status_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX activity_import_status_idx ON public.activity_import USING btree (status, created_at);


--
-- Name: activity_job_status_kind_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX activity_job_status_kind_idx ON public.activity_job USING btree (kind, status, available_at);


--
-- Name: activity_notification_dedupe_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX activity_notification_dedupe_idx ON public.activity_notification USING btree (deduplication_key) WHERE (deduplication_key IS NOT NULL);


--
-- Name: activity_notification_recipient_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX activity_notification_recipient_idx ON public.activity_notification USING btree (recipient_id, status, created_at DESC);


--
-- Name: activity_notification_redrive_event_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX activity_notification_redrive_event_idx ON public.activity_notification_redrive USING btree (event_id, requested_at DESC);


--
-- Name: activity_qso_activation_time_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX activity_qso_activation_time_idx ON public.activity_qso USING btree (activation_id, occurred_at DESC);


--
-- Name: activity_qso_activation_valid_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX activity_qso_activation_valid_idx ON public.activity_qso USING btree (activation_id, occurred_at DESC) WHERE (status <> 'VOID'::text);


--
-- Name: activity_qso_callsign_time_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX activity_qso_callsign_time_idx ON public.activity_qso USING btree (worked_callsign, occurred_at DESC);


--
-- Name: activity_qso_correction_status_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX activity_qso_correction_status_idx ON public.activity_qso_correction USING btree (status, created_at);


--
-- Name: activity_qso_entity_time_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX activity_qso_entity_time_idx ON public.activity_qso USING btree (worked_entity_id, occurred_at DESC);


--
-- Name: activity_qso_hunter_time_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX activity_qso_hunter_time_idx ON public.activity_qso USING btree (hunter_id, occurred_at DESC);


--
-- Name: activity_qso_operator_time_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX activity_qso_operator_time_idx ON public.activity_qso USING btree (operator_id, occurred_at DESC);


--
-- Name: activity_qso_programme_time_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX activity_qso_programme_time_idx ON public.activity_qso USING btree (programme_slug, occurred_at DESC);


--
-- Name: activity_qso_time_brin_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX activity_qso_time_brin_idx ON public.activity_qso USING brin (occurred_at);


--
-- Name: activity_qso_worked_entity_valid_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX activity_qso_worked_entity_valid_idx ON public.activity_qso USING btree (worked_entity_id, occurred_at DESC) WHERE (status <> 'VOID'::text);


--
-- Name: activity_subject_aggregate_qso_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX activity_subject_aggregate_qso_idx ON public.activity_subject_aggregate USING btree (programme_slug, category, qso_count DESC);


--
-- Name: activity_work_dead_letter_unresolved_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX activity_work_dead_letter_unresolved_idx ON public.activity_work_dead_letter USING btree (dead_lettered_at) WHERE (resolved_at IS NULL);


--
-- Name: activity_work_redrive_job_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX activity_work_redrive_job_idx ON public.activity_work_redrive_audit USING btree (job_id, requested_at DESC);


--
-- Name: outbox_dead_letter_unresolved_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX outbox_dead_letter_unresolved_idx ON public.dead_letter_event USING btree (dead_lettered_at) WHERE (resolved_at IS NULL);


--
-- Name: outbox_pending_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX outbox_pending_idx ON public.outbox_event USING btree (available_at, occurred_at) WHERE (published_at IS NULL);


--
-- Name: outbox_redrive_audit_event_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX outbox_redrive_audit_event_idx ON public.outbox_redrive_audit USING btree (event_id, requested_at DESC);


--
-- Name: activity_award_issuance activity_award_issuance_award_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.activity_award_issuance
    ADD CONSTRAINT activity_award_issuance_award_id_fkey FOREIGN KEY (award_id) REFERENCES public.activity_award_definition(id);


--
-- Name: activity_award_issuance activity_award_issuance_request_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.activity_award_issuance
    ADD CONSTRAINT activity_award_issuance_request_id_fkey FOREIGN KEY (request_id) REFERENCES public.activity_award_request(id);


--
-- Name: activity_award_progress activity_award_progress_award_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.activity_award_progress
    ADD CONSTRAINT activity_award_progress_award_id_fkey FOREIGN KEY (award_id) REFERENCES public.activity_award_definition(id);


--
-- Name: activity_award_request activity_award_request_award_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.activity_award_request
    ADD CONSTRAINT activity_award_request_award_id_fkey FOREIGN KEY (award_id) REFERENCES public.activity_award_definition(id);


--
-- Name: activity_import activity_import_activation_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.activity_import
    ADD CONSTRAINT activity_import_activation_id_fkey FOREIGN KEY (activation_id) REFERENCES public.activity_activation(id);


--
-- Name: activity_notification_redrive activity_notification_redrive_event_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.activity_notification_redrive
    ADD CONSTRAINT activity_notification_redrive_event_id_fkey FOREIGN KEY (event_id) REFERENCES public.dead_letter_event(event_id);


--
-- Name: activity_qso activity_qso_activation_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.activity_qso
    ADD CONSTRAINT activity_qso_activation_id_fkey FOREIGN KEY (activation_id) REFERENCES public.activity_activation(id);


--
-- Name: activity_qso_correction activity_qso_correction_qso_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.activity_qso_correction
    ADD CONSTRAINT activity_qso_correction_qso_id_fkey FOREIGN KEY (qso_id) REFERENCES public.activity_qso(id);


--
-- Name: activity_work_redrive_audit activity_work_redrive_audit_job_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.activity_work_redrive_audit
    ADD CONSTRAINT activity_work_redrive_audit_job_id_fkey FOREIGN KEY (job_id) REFERENCES public.activity_job(id);


--
-- PostgreSQL database dump complete
--



-- Record that this immutable schema baseline has been installed or adopted.
CREATE TABLE public.myota_schema_migration (
  migration_id text PRIMARY KEY,
  checksum text,
  application_mode text NOT NULL CHECK (application_mode IN ('applied', 'adopted')),
  applied_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO public.myota_schema_migration (migration_id, application_mode)
VALUES ('baseline-v1', 'applied');
