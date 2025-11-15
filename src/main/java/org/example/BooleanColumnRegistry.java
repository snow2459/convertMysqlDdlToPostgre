package org.example;

import java.util.*;

/**
 * 维护需要强制转换为 boolean 的列清单，支持全局列与表级别列。
 */
public final class BooleanColumnRegistry {

    private static final Set<String> GLOBAL_COLUMNS = new HashSet<>(List.of(
            "is_force_update_password"
    ));

    private static final Map<String, Set<String>> TABLE_SPECIFIC_COLUMNS = new HashMap<>();

    static {
        // 系统服务
        registerTableColumn("sys_user", "is_force_update_password");
        // 工作流
        registerTableColumn("act_hi_caseactinst", "required_");
        registerTableColumn("act_hi_detail", "initial_");
        registerTableColumn("act_ge_bytearray", "generated_");
        registerTableColumn("act_re_procdef", "has_start_form_key_");
        registerTableColumn("act_re_procdef", "startable_");
        registerTableColumn("bpm_de_model", "status");
        registerTableColumn("bpm_de_model", "global_mark");
        registerTableColumn("bpm_de_model", "batch_support");
        registerTableColumn("bpm_de_model", "application_advice_support");
        registerTableColumn("bpm_de_model", "applicant_assign_support");
        registerTableColumn("bpm_event", "global_mark");
        registerTableColumn("bpm_event", "global_trigger_mark");
        registerTableColumn("bpm_event", "global_trigger_mark");
        registerTableColumn("enable", "global_trigger_mark");
        registerTableColumn("bpm_proc_button", "global_mark");
        registerTableColumn("bpm_proc_button", "custom_mark");
        registerTableColumn("bpm_proc_button", "message_required");
        registerTableColumn("bpm_proc_button", "edited");
        registerTableColumn("bpm_proc_button", "selected");
        registerTableColumn("bpm_proc_def", "approve_batch");
        registerTableColumn("bpm_proc_def", "global_mark");
        registerTableColumn("bpm_proc_def", "enable");
        registerTableColumn("bpm_proc_def", "batch_support");
        registerTableColumn("bpm_proc_def", "application_advice_support");
        registerTableColumn("bpm_proc_def", "applicant_assign_support");
        registerTableColumn("bpm_re_node", "can_save");
        registerTableColumn("bpm_re_node", "feedback_rule");
        registerTableColumn("bpm_re_node", "revoke_rule_next_todo");
        registerTableColumn("bpm_re_node", "revoke_rule_permit_preemption");
        registerTableColumn("bpm_re_node", "rejected_permit_direct_send");
        registerTableColumn("bpm_re_node", "signature_rule_permit_assigned");
        registerTableColumn("bpm_re_node", "cc_rule_permit_assigned");
        registerTableColumn("bpm_re_node", "cc_assigned_required");
        registerTableColumn("bpm_re_node", "cc_assigned_scoped");
        registerTableColumn("bpm_re_node", "empty_approve_skip_rule");
        registerTableColumn("bpm_re_node", "same_approve_skip_rule");
        registerTableColumn("act_ru_case_execution", "required_");
        registerTableColumn("act_ru_case_sentry_part", "satisfied_");
        registerTableColumn("act_ru_execution", "is_active_");
        registerTableColumn("act_ru_execution", "is_concurrent_");
        registerTableColumn("act_ru_execution", "is_scope_");
        registerTableColumn("act_ru_execution", "is_event_scope_");
        registerTableColumn("act_ru_job", "exclusive_");
        registerTableColumn("act_ru_variable", "is_concurrent_local_");
        registerTableColumn("bpm_re_node", "multi_reject");
        registerTableColumn("bpm_re_node", "enable_signature");

    }

    private BooleanColumnRegistry() {
    }

    public static boolean isBooleanColumn(String columnName) {
        return isBooleanColumn(null, columnName);
    }

    public static boolean isBooleanColumn(String tableName, String columnName) {
        if (columnName == null) {
            return false;
        }
        String normalizedColumn = normalizeColumn(columnName);
        if (tableName != null) {
            String normalizedTable = normalizeTableName(tableName);
            if (matchesTableSpecific(normalizedTable, normalizedColumn)) {
                return true;
            }
        }
        return GLOBAL_COLUMNS.contains(normalizedColumn);
    }

    public static Set<String> listedColumns() {
        return Collections.unmodifiableSet(GLOBAL_COLUMNS);
    }

    private static boolean matchesTableSpecific(String normalizedTableName, String normalizedColumnName) {
        if (normalizedTableName == null) {
            return false;
        }
        Set<String> columns = TABLE_SPECIFIC_COLUMNS.get(normalizedTableName);
        if (columns != null && columns.contains(normalizedColumnName)) {
            return true;
        }
        String simpleName = simpleTableName(normalizedTableName);
        if (!simpleName.equals(normalizedTableName)) {
            columns = TABLE_SPECIFIC_COLUMNS.get(simpleName);
            return columns != null && columns.contains(normalizedColumnName);
        }
        return false;
    }

    private static void registerTableColumn(String tableName, String columnName) {
        String normalizedTable = normalizeTableName(tableName);
        TABLE_SPECIFIC_COLUMNS
                .computeIfAbsent(normalizedTable, key -> new HashSet<>())
                .add(normalizeColumn(columnName));
    }

    private static String normalizeColumn(String columnName) {
        return columnName
                .replace("\"", "")
                .replace("`", "")
                .trim()
                .toLowerCase(Locale.ROOT);
    }

    private static String normalizeTableName(String tableName) {
        if (tableName == null) {
            return null;
        }
        return tableName
                .replace("\"", "")
                .replace("`", "")
                .trim()
                .toLowerCase(Locale.ROOT);
    }

    private static String simpleTableName(String normalizedTable) {
        if (normalizedTable == null) {
            return null;
        }
        int idx = normalizedTable.lastIndexOf('.');
        if (idx >= 0 && idx + 1 < normalizedTable.length()) {
            return normalizedTable.substring(idx + 1);
        }
        return normalizedTable;
    }
}
