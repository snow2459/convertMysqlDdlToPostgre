CREATE TABLE bpm_proc_button (
    id varchar(32) PRIMARY KEY,
    button_name varchar(255) NULL,
    code varchar(64) NOT NULL,
    button_alias varchar(255) NULL,
    url varchar(255) NULL,
    order_no int NULL,
    remark varchar(255) NULL,
    icon varchar(255) NULL,
    global_mark boolean DEFAULT TRUE,
    custom_mark smallint NULL,
    button_style varchar(255) NULL,
    message_required smallint DEFAULT '1',
    edited smallint DEFAULT '1',
    selected smallint DEFAULT '0',
    method_name varchar(255) NULL,
    button_type int DEFAULT '1',
    parent_id varchar(64) DEFAULT '-1',
    model_code varchar(255) NULL,
    delete_flag smallint NULL,
    delete_time timestamp NULL,
    delete_user_id varchar(32) NULL,
    create_user_id varchar(32) NULL,
    create_time timestamp NULL,
    create_org_id varchar(32) NULL,
    update_time timestamp NULL,
    update_user_id varchar(32) NULL
);
COMMENT ON COLUMN bpm_proc_button.message_required IS '审批意见是否必填';
COMMENT ON COLUMN bpm_proc_button.edited IS '能否编辑';
COMMENT ON COLUMN bpm_proc_button.selected IS '是否选中';
COMMENT ON COLUMN bpm_proc_button.method_name IS '方法名';
COMMENT ON COLUMN bpm_proc_button.button_type IS '类型0类型1按钮';
COMMENT ON COLUMN bpm_proc_button.parent_id IS '父节点id';
CREATE TABLE bpm_proc_def (
    id varchar(32) PRIMARY KEY,
    form_code varchar(255) NULL,
    form_name varchar(255) NULL,
    proc_def_id varchar(64) NULL,
    model_code varchar(255) NULL,
    model_name varchar(255) NULL,
    version int NULL,
    model_json text,
    model_xml text,
    model_image bytea,
    deploy_id varchar(64) NULL,
    deploy_time timestamp NULL,
    word_template text,
    task_title varchar(255) NULL,
    approve_batch smallint NULL,
    show_type int DEFAULT '0',
    global_mark smallint NULL,
    application_type varchar(2) NULL,
    url varchar(255) NULL,
    enable smallint NULL,
    order_no int NULL,
    tenant_id varchar(32) NULL,
    remark varchar(255) NULL,
    batch_support smallint DEFAULT '0',
    application_advice_support smallint DEFAULT '0',
    applicant_assign_support smallint DEFAULT '0',
    delete_flag smallint NULL,
    delete_time timestamp NULL,
    delete_user_id varchar(32) NULL,
    create_user_id varchar(32) NULL,
    create_time timestamp NULL,
    create_org_id varchar(32) NULL,
    update_time timestamp NULL,
    update_user_id varchar(32) NULL
);
COMMENT ON COLUMN bpm_proc_def.task_title IS '任务标题';
COMMENT ON COLUMN bpm_proc_def.show_type IS '展示类型0tab页1合并';
COMMENT ON COLUMN bpm_proc_def.batch_support IS '批量支持';
COMMENT ON COLUMN bpm_proc_def.application_advice_support IS '是否支持发起时填写申请意见';
COMMENT ON COLUMN bpm_proc_def.applicant_assign_support IS '是否支持发起人指派节点审批人';
