export const HollerPlugin = async ({ client, serverUrl }) => {
  const registrations = new Map()
  const pendingContext = new Map()
  const maxErrorDetail = 512

  const errorDetail = (error) => {
    const detail = error instanceof Error ? error.message : String(error)
    return detail.length > maxErrorDetail
      ? `${detail.slice(0, maxErrorDetail - 3)}...`
      : detail
  }

  const log = async (level, message, extra = {}) => {
    try {
      await client.app.log({
        body: { service: "holler", level, message, extra },
      })
    } catch {
      // Logging must never block the host agent.
    }
  }

  const lifecycle = async (command, sessionID) => {
    const binary = process.env.HOLLER_BIN || "holler"
    const child = Bun.spawn([binary, command, "--harness", "opencode"], {
      env: {
        ...process.env,
        HOLLER_OPENCODE_SERVER: serverUrl.toString().replace(/\/$/, ""),
      },
      stdin: "pipe",
      stdout: "pipe",
      stderr: "pipe",
    })
    child.stdin.write(JSON.stringify({ session_id: sessionID }))
    child.stdin.end()
    const [stdout, stderr, exitCode] = await Promise.all([
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
      child.exited,
    ])
    if (exitCode !== 0) {
      throw new Error(`${command} exited ${exitCode}: ${stderr.trim()}`)
    }
    if (command !== "hook") return ""
    const output = JSON.parse(stdout)
    return output?.hookSpecificOutput?.additionalContext || ""
  }

  const register = async (sessionID, refresh = false) => {
    if (!sessionID) return ""
    if (!refresh && registrations.has(sessionID)) return registrations.get(sessionID)
    const operation = lifecycle("hook", sessionID)
      .then((context) => {
        if (context) pendingContext.set(sessionID, context)
        return context
      })
      .catch(async (error) => {
        registrations.delete(sessionID)
        await log("warn", "Holler registration degraded", {
          sessionID,
          detail: errorDetail(error),
        })
        const degraded = "Holler connector state is DEGRADED because OpenCode lifecycle registration failed. Continue independent work where safe and ask the operator to run holler connector doctor."
        pendingContext.set(sessionID, degraded)
        return degraded
      })
    registrations.set(sessionID, operation)
    return operation
  }

  const unregister = async (sessionID) => {
    registrations.delete(sessionID)
    pendingContext.delete(sessionID)
    try {
      await lifecycle("session-end", sessionID)
    } catch (error) {
      await log("warn", "Holler session expiry degraded", {
        sessionID,
        detail: errorDetail(error),
      })
    }
  }

  return {
    "chat.message": async ({ sessionID }) => {
      await register(sessionID)
    },

    "experimental.chat.system.transform": async ({ sessionID }, output) => {
      if (!sessionID) return
      await register(sessionID)
      const context = pendingContext.get(sessionID)
      if (context) {
        output.system.push(context)
        pendingContext.delete(sessionID)
      }
    },

    "experimental.session.compacting": async ({ sessionID }, output) => {
      const context = await register(sessionID, true)
      if (context) {
        output.context.push(context)
        pendingContext.delete(sessionID)
      }
    },

    event: async ({ event }) => {
      if (event.type === "session.created" || event.type === "session.updated") {
        await register(event.properties?.info?.id)
      } else if (event.type === "message.updated") {
        await register(event.properties?.info?.sessionID)
      } else if (event.type === "session.deleted") {
        await unregister(event.properties?.info?.id)
      }
    },
  }
}
