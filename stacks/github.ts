import * as Alchemy from "alchemy";
import * as Cloudflare from "alchemy/Cloudflare";
import * as GitHub from "alchemy/GitHub";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";

const GITHUB_OWNER = "peterje";
const GITHUB_REPOSITORY = "chemistry";

/**
 * Administrative bootstrap stack that provisions scoped Cloudflare credentials
 * for the Chemistry GitHub Actions pipeline.
 */
export default Alchemy.Stack(
  "ChemistryGitHub",
  {
    providers: Layer.mergeAll(Cloudflare.providers(), GitHub.providers()),
    state: Cloudflare.state(),
  },
  Effect.gen(function* () {
    const { accountId } = yield* yield* Cloudflare.CloudflareEnvironment;
    const apiToken = yield* Cloudflare.ApiToken.AccountApiToken("ChemistryCI", {
      accountId,
      policies: [
        {
          effect: "allow",
          permissionGroups: [
            "Workers Scripts Write",
            "Account Settings Write",
            "Secrets Store Write",
          ],
          resources: {
            [`com.cloudflare.api.account.${accountId}`]: "*",
          },
        },
      ],
    });

    yield* GitHub.Secret("cloudflare-api-token", {
      owner: GITHUB_OWNER,
      repository: GITHUB_REPOSITORY,
      name: "CLOUDFLARE_API_TOKEN",
      value: apiToken.value,
    });
    yield* GitHub.Secret("cloudflare-account-id", {
      owner: GITHUB_OWNER,
      repository: GITHUB_REPOSITORY,
      name: "CLOUDFLARE_ACCOUNT_ID",
      value: Redacted.make(accountId),
    });
  }),
);
