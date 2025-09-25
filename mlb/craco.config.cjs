// craco.config.js
console.log("🛠 CRACO CONFIG LOADED");

const path = require("path");
const webpack = require("webpack");

module.exports = {
  webpack: {
    alias: {
      "@shared": path.resolve(__dirname, "backend/scripts/shared"),
    },
    configure: (webpackConfig) => {
      // 🧩 Add polyfills for zlib, stream, util
      webpackConfig.resolve = {
        ...webpackConfig.resolve,
        fallback: {
          ...webpackConfig.resolve.fallback,
          zlib: require.resolve("browserify-zlib"),
          stream: require.resolve("stream-browserify"),
          util: require.resolve("util/"),
        },
      };

      // 💡 Remove CRA's module scope restrictions
      const scopePluginIndex = webpackConfig.resolve.plugins.findIndex(
        ({ constructor }) =>
          constructor && constructor.name === "ModuleScopePlugin"
      );
      if (scopePluginIndex !== -1) {
        webpackConfig.resolve.plugins.splice(scopePluginIndex, 1);
      }

      // 💡 Add path alias to allow cleaner imports
      webpackConfig.resolve.alias = {
        ...(webpackConfig.resolve.alias || {}),
        "@shared": path.resolve(__dirname, "backend/scripts/shared"),
      };

      // 💡 Add ProvidePlugin for process and Buffer
      webpackConfig.plugins = [
        ...(webpackConfig.plugins || []),
        new webpack.ProvidePlugin({
          process: "process/browser",
          Buffer: ["buffer", "Buffer"],
        }),
      ];

      return webpackConfig;
    },
  },
};
