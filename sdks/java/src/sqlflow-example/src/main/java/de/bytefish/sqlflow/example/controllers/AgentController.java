package de.bytefish.sqlflow.example.controllers;

import de.bytefish.sqlflow.core.ISqlFlow;
import de.bytefish.sqlflow.core.models.EmitEventOptions;
import de.bytefish.sqlflow.core.models.SpawnOptions;
import de.bytefish.sqlflow.core.models.SpawnResult;
import de.bytefish.sqlflow.example.models.AgentTask;
import de.bytefish.sqlflow.example.models.HumanApproval;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/agent")
public class AgentController {

    private final ISqlFlow sqlFlow;

    public AgentController(ISqlFlow sqlFlow) {
        this.sqlFlow = sqlFlow;
    }

    @PostMapping("/start")
    public Map<String, String> startAgent(
            @RequestBody AgentTask task) {

        SpawnResult result = sqlFlow.spawn(
                new SpawnOptions(
                        "ai-agent-queue", null, null, null),
                "solve-bug",
                task);

        return Map.of(
                "RunId", result.runId(),
                "TaskId", result.taskId(),
                "Status",
                "Agent dispatched to fix Issue #" + task.issueId());
    }

    @PostMapping("/review/{issueId}/{correlationId}")
    public Map<String, String> review(
            @PathVariable String issueId,
            @PathVariable String correlationId,
            @RequestBody HumanApproval approval) {

        String eventName =
                "agent-approval:" + issueId + ":" + correlationId;

        sqlFlow.emitEvent(
                new EmitEventOptions("ai-agent-queue"),
                eventName,
                approval);

        String message = approval.approved()
                ? "Fix for " + correlationId
                    + " approved. Agent is now completing its work."
                : "Fix for " + correlationId
                    + " rejected. Agent tries again with feedback: '"
                    + approval.reason() + "'";

        return Map.of("Message", message);
    }
}