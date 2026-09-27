# Testing the AI Agent Job: HTTP Queries using ijhttp #

## Getting the Tooling right ##

It's not stone age. I want to use tooling to fire my HTTP requests. There's somewhat of a standard 
established for tooling, which is the `http` format for HTTP requests.

And while it's easy to use `*.http` files with Visual Studio, IntelliJ doesn't come with a UI 
in it's Community Edition. But do not fear, you don't have to fight `curl`. JetBrains offers a 
CLI called `ijhttp` we can use.

We start by downloading it off the JetBrains pages:

```powershell
curl.exe -f -L -o ijhttp.zip "https://jb.gg/ijhttp/latest"
```

And extract it to a folder `Tools` in the User Profile:

```
Expand-Archive .\ijhttp.zip -DestinationPath "$env:USERPROFILE\Tools\ijhttp"
```

We can then add `ijhttp` to the search `Path` in Windows:

```powershell
$folder = "$env:USERPROFILE\Tools\ijhttp\ijhttp"
$userPath = [Environment]::GetEnvironmentVariable("Path", "User")

[Environment]::SetEnvironmentVariable("Path", "$userPath;$folder", "User")
```

## The *.http File with the Requests ##

```java
@baseUrl = https://localhost:5000
@issueId = 12345
@delayMs = 30000

### Start the Agent Job
# @name startAgent
POST {{baseUrl}}/agent/start
Content-Type: application/json

{
  "issue_id": "{{issueId}}"
}

> {%
    client.test("Agent was started", function () {
        client.assert(response.status === 200, "Expected HTTP 200");
        client.assert(response.body.taskId, "Response does not contain taskId");
    });

    client.global.set("taskId", response.body.taskId);
    client.log("Stored taskId: " + response.body.taskId);
%}

### Reject the first attempt after a delay
< {%
    await sleep(Number(request.variables.get("delayMs")));
%}
POST {{baseUrl}}/agent/review/{{issueId}}/{{taskId}}-attempt-1
Content-Type: application/json

{
  "approved": false,
  "reason": "This is way too simple, add a better error handling strategy!"
}

> {%
    client.test("First review was submitted", function () {
        client.assert(response.status === 200, "Expected HTTP 200");
    });
%}

### Approve the second attempt after another delay
< {%
    await sleep(Number(request.variables.get("delayMs")));
%}
POST {{baseUrl}}/agent/review/{{issueId}}/{{taskId}}-attempt-2
Content-Type: application/json

{
  "approved": true,
  "reason": "Now, this looks good!"
}

> {%
    client.test("Second review was submitted", function () {
        client.assert(response.status === 200, "Expected HTTP 200");
    });
%}
```

## Analyzing the Log Output ##

After starting the Backend we can see the Postgres container being booted:

```
2026-08-16T10:37:08.743+02:00  INFO 24396 --- [           main] tc.postgres:18                           : Creating container for image: postgres:18
2026-08-16T10:37:08.807+02:00  INFO 24396 --- [           main] tc.postgres:18                           : Container postgres:18 is starting: b98a7b3788ed1e6cf7400ea0cc0d446c03f9aa36b61de449f4fd5e9dd7e8e82d
2026-08-16T10:37:09.877+02:00  INFO 24396 --- [           main] tc.postgres:18                           : Container postgres:18 started in PT1.1334854S
2026-08-16T10:37:09.878+02:00  INFO 24396 --- [           main] tc.postgres:18                           : Container is started (JDBC URL: jdbc:postgresql://localhost:44757/test?loggerLevel=OFF)
2026-08-16T10:37:09.884+02:00  INFO 24396 --- [           main] org.testcontainers.ext.ScriptUtils       : Executing database script from ssf-postgres.sql
2026-08-16T10:37:10.001+02:00  INFO 24396 --- [           main] org.testcontainers.ext.ScriptUtils       : Executed database script from ssf-postgres.sql in 116 ms.
```

And the Worker for the `ai-agent-queue` being created:

```
2026-08-16T10:37:10.141+02:00  INFO 24396 --- [    virtual-103] d.b.sqlflow.core.workers.SqlFlowWorker   : SqlFlow Worker [spring-worker-1] started for queue 'ai-agent-queue'
```

The Backend is ready to perform. So let's give it something to eat.

We'll switch to the IntelliJ Terminal:

* `File → Settings → Tools → Terminal`

We'll then run out `*.http` script using `ijhttp -L VERBOSE agent-requests.http`. 

The first request for fixing an issue `12345` is sent:

```
PS sqlflow-example\requests> ijhttp -L VERBOSE agent-requests.http
┌─────────────────────────────────────────────────────────────────────────────┐
│                      Running IntelliJ HTTP Client with                      │
├────────────────────────┬────────────────────────────────────────────────────┤
│         Files          │ agent-requests.http                                │
├────────────────────────┼────────────────────────────────────────────────────┤
│   Public Environment   │                                                    │
├────────────────────────┼────────────────────────────────────────────────────┤
│  Private Environment   │                                                    │
└────────────────────────┴────────────────────────────────────────────────────┘
Request 'startAgent' POST http://localhost:5000/agent/start
= request =>
POST http://localhost:5000/agent/start
Content-Type: application/json
Content-Length: 25
User-Agent: IntelliJ HTTP Client/CLI 2026.1
Accept-Encoding: br, deflate, gzip, x-gzip
Accept: */*

{
  "issue_id": "12345"
}

###

<= response =
HTTP/1.1 200
Content-Type: application/json
Content-Length: 144
Date: Sun, 16 Aug 2026 08:44:33 GMT

{"taskId":"d51e000b-b1cc-4ac2-a30a-5521ae3f1c15","runId":"6b565cc1-872c-4cd1-b4b9-a84c5ba2e4b5","status":"Agent dispatched to fix Issue #12345"}

Response code: 200; Time: 448ms (448 ms); Content length: 144 bytes (144 B)
```

In the Backend we can see our fictional agent doing its fictional work:

```
2026-08-16T10:45:36.259+02:00  INFO 24396 --- [onPool-worker-3] d.b.s.e.workflows.AutonomousAgentJob     : Agent starts researching ticket 12345
2026-08-16T10:45:36.259+02:00  INFO 24396 --- [onPool-worker-3] d.b.s.e.workflows.AutonomousAgentJob     : Attempt 1/3: Generating a fix based on: Initial Attempt
2026-08-16T10:45:36.259+02:00  INFO 24396 --- [onPool-worker-3] d.b.s.e.workflows.AutonomousAgentJob     : Review for d51e000b-b1cc-4ac2-a30a-5521ae3f1c15-attempt-1 has been requested. Agent goes idle and waits for the code review...
```

We can see it goes idle and requests a human review. But the ficional fix looks way too simple, so we'll reject it:

```
Request 'Reject the first attempt after a delay' POST http://localhost:5000/agent/review/12345/d51e000b-b1cc-4ac2-a30a-5521ae3f1c15-attempt-1
= request =>
POST http://localhost:5000/agent/review/12345/d51e000b-b1cc-4ac2-a30a-5521ae3f1c15-attempt-1
Content-Type: application/json
Content-Length: 100
User-Agent: IntelliJ HTTP Client/CLI 2026.1
Accept-Encoding: br, deflate, gzip, x-gzip
Accept: */*

{
  "approved": false,
  "reason": "This is way too simple, add a better error handling strategy!"
}

###

<= response =
HTTP/1.1 200
Content-Type: application/json
Content-Length: 175
Date: Sun, 16 Aug 2026 08:45:05 GMT

{"message":"Fix for d51e000b-b1cc-4ac2-a30a-5521ae3f1c15-attempt-1 rejected. Agent tries again with feedback: 'This is way too simple, add a better error handling strategy!'"}

Response code: 200; Time: 26ms (26 ms); Content length: 175 bytes (175 B)
```

We can see the Backend receiving the request and the agent is generating another fix, based on our feedback:

```
2026-08-16T10:45:36.259+02:00  WARN 24396 --- [onPool-worker-3] d.b.s.e.workflows.AutonomousAgentJob     : Attempt 1 has been rejected: This is way too simple, add a better error handling strategy!
2026-08-16T10:45:36.259+02:00  INFO 24396 --- [onPool-worker-3] d.b.s.e.workflows.AutonomousAgentJob     : Attempt 2/3: Generating a fix based on: This is way too simple, add a better error handling strategy!
2026-08-16T10:45:36.259+02:00  INFO 24396 --- [onPool-worker-3] d.b.s.e.workflows.AutonomousAgentJob     : Review for d51e000b-b1cc-4ac2-a30a-5521ae3f1c15-attempt-2 has been requested. Agent goes idle and waits for the code review...
```

Let's not spend too many fictional tokens on this and accept the fix:

```
Request 'Approve the second attempt after another delay' POST http://localhost:5000/agent/review/12345/d51e000b-b1cc-4ac2-a30a-5521ae3f1c15-attempt-2
= request =>
POST http://localhost:5000/agent/review/12345/d51e000b-b1cc-4ac2-a30a-5521ae3f1c15-attempt-2
Content-Type: application/json
Content-Length: 59
User-Agent: IntelliJ HTTP Client/CLI 2026.1
Accept-Encoding: br, deflate, gzip, x-gzip
Accept: */*

{
  "approved": true,
  "reason": "Now, this looks good!"
}

###

<= response =
HTTP/1.1 200
Content-Type: application/json
Content-Length: 112
Date: Sun, 16 Aug 2026 08:45:35 GMT

{"message":"Fix for d51e000b-b1cc-4ac2-a30a-5521ae3f1c15-attempt-2 approved. Agent is now completing its work."}

Response code: 200; Time: 19ms (19 ms); Content length: 112 bytes (112 B)
```

In the logs we can see a happy agent completing the mission and creating a PR:

```
2026-08-16T10:45:36.259+02:00  INFO 24396 --- [onPool-worker-3] d.b.s.e.workflows.AutonomousAgentJob     : Fix approved. Creating Pull Request...
2026-08-16T10:45:36.261+02:00  INFO 24396 --- [onPool-worker-3] d.b.s.e.s.impl.DefaultGitHubService      : GitHub: PR for Issue #12345 has been created...
2026-08-16T10:45:37.467+02:00  INFO 24396 --- [onPool-worker-3] d.b.s.e.workflows.AutonomousAgentJob     : Mission accomplished, the PR has been created: https://github.com/company/repo/pull/7421
```