import { RegistryProvider } from "@effect/atom-react";
import { HeadContent, Outlet, Scripts, createRootRoute } from "@tanstack/react-router";
import type { ReactNode } from "react";
import "../styles.css";

/** Root document route installing the Effect Atom registry and shared shell. */
export const Route = createRootRoute({
  head: () => ({
    meta: [
      { charSet: "utf-8" },
      { name: "viewport", content: "width=device-width, initial-scale=1" },
      { title: "Alchemy Effect Agent" },
    ],
  }),
  component: RootComponent,
});

function RootComponent() {
  return (
    <Document>
      <RegistryProvider>
        <Outlet />
      </RegistryProvider>
    </Document>
  );
}

function Document({ children }: Readonly<{ children: ReactNode }>) {
  return (
    <html lang="en">
      <head>
        <HeadContent />
      </head>
      <body>
        {children}
        <Scripts />
      </body>
    </html>
  );
}
