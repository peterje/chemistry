import type {} from "@tanstack/react-start";
import { Route as rootRouteImport } from "./routes/__root.tsx";
import { Route as IndexRouteImport } from "./routes/index.tsx";
import { Route as RpcRouteImport } from "./routes/rpc.ts";
import { Route as WsRouteImport } from "./routes/ws.ts";

const noPendingRouteComponent = () => null;

const rpcRouteOptions = {
  id: "/rpc",
  path: "/rpc",
  getParentRoute: () => rootRouteImport,
  pendingComponent: noPendingRouteComponent,
};
const wsRouteOptions = {
  id: "/ws",
  path: "/ws",
  getParentRoute: () => rootRouteImport,
  pendingComponent: noPendingRouteComponent,
};
const indexRouteOptions = {
  id: "/",
  path: "/",
  getParentRoute: () => rootRouteImport,
  pendingComponent: noPendingRouteComponent,
};

const RpcRoute = RpcRouteImport.update(rpcRouteOptions);
const WsRoute = WsRouteImport.update(wsRouteOptions);
const IndexRoute = IndexRouteImport.update(indexRouteOptions);

interface FileRoutesByFullPath {
  readonly "/": typeof IndexRoute;
  readonly "/rpc": typeof RpcRoute;
  readonly "/ws": typeof WsRoute;
}

interface FileRoutesByTo {
  readonly "/": typeof IndexRoute;
  readonly "/rpc": typeof RpcRoute;
  readonly "/ws": typeof WsRoute;
}

interface FileRoutesById {
  readonly __root__: typeof rootRouteImport;
  readonly "/": typeof IndexRoute;
  readonly "/rpc": typeof RpcRoute;
  readonly "/ws": typeof WsRoute;
}

interface FileRouteTypes {
  readonly fileRoutesByFullPath: FileRoutesByFullPath;
  readonly fullPaths: "/" | "/rpc" | "/ws";
  readonly fileRoutesByTo: FileRoutesByTo;
  readonly to: "/" | "/rpc" | "/ws";
  readonly id: "__root__" | "/" | "/rpc" | "/ws";
  readonly fileRoutesById: FileRoutesById;
}

interface RootRouteChildren {
  readonly IndexRoute: typeof IndexRoute;
  readonly RpcRoute: typeof RpcRoute;
  readonly WsRoute: typeof WsRoute;
}

declare module "@tanstack/react-router" {
  interface FileRoutesByPath {
    readonly "/ws": {
      readonly id: "/ws";
      readonly path: "/ws";
      readonly fullPath: "/ws";
      readonly preLoaderRoute: typeof WsRouteImport;
      readonly parentRoute: typeof rootRouteImport;
    };
    readonly "/rpc": {
      readonly id: "/rpc";
      readonly path: "/rpc";
      readonly fullPath: "/rpc";
      readonly preLoaderRoute: typeof RpcRouteImport;
      readonly parentRoute: typeof rootRouteImport;
    };
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
  RpcRoute,
  WsRoute,
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
