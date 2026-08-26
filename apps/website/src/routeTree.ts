import type {} from "@tanstack/react-start";
import { Route as rootRouteImport } from "./routes/__root.tsx";
import { Route as IndexRouteImport } from "./routes/index.tsx";

const noPendingRouteComponent = () => null;

const indexRouteOptions = {
  id: "/",
  path: "/",
  getParentRoute: () => rootRouteImport,
  pendingComponent: noPendingRouteComponent,
};

const IndexRoute = IndexRouteImport.update(indexRouteOptions);

interface FileRoutesByFullPath {
  readonly "/": typeof IndexRoute;
}

interface FileRoutesByTo {
  readonly "/": typeof IndexRoute;
}

interface FileRoutesById {
  readonly __root__: typeof rootRouteImport;
  readonly "/": typeof IndexRoute;
}

interface FileRouteTypes {
  readonly fileRoutesByFullPath: FileRoutesByFullPath;
  readonly fullPaths: "/";
  readonly fileRoutesByTo: FileRoutesByTo;
  readonly to: "/";
  readonly id: "__root__" | "/";
  readonly fileRoutesById: FileRoutesById;
}

interface RootRouteChildren {
  readonly IndexRoute: typeof IndexRoute;
}

declare module "@tanstack/react-router" {
  interface FileRoutesByPath {
    readonly "/": {
      readonly id: "/";
      readonly path: "/";
      readonly fullPath: "/";
      readonly preLoaderRoute: typeof IndexRouteImport;
      readonly parentRoute: typeof rootRouteImport;
    };
  }
}

const rootRouteChildren: RootRouteChildren = {
  IndexRoute,
};

/** Strictly typed application route tree without generated type assertions. */
export const routeTree = rootRouteImport
  ._addFileChildren(rootRouteChildren)
  ._addFileTypes<FileRouteTypes>();

import type { getRouter } from "./router.tsx";

declare module "@tanstack/react-start" {
  interface Register {
    ssr: true;
    router: Awaited<ReturnType<typeof getRouter>>;
  }
}
