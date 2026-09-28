import pino from "pino";

type ErrorLogListener = (args: unknown[]) => void;

let errorLogListener: ErrorLogListener | null = null;

export function setErrorLogListener(listener: ErrorLogListener | null): void {
  errorLogListener = listener;
}

export const logger = pino({
  level: process.env.LOG_LEVEL ?? "warn",
  hooks: {
    logMethod(args, method, level) {
      if (level >= 50 && errorLogListener) {
        try {
          errorLogListener(args as unknown[]);
        } catch {
          // Detection must not break logging.
        }
      }
      method.apply(this, args);
    },
  },
});
