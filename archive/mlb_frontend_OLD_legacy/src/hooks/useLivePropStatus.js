import { useEffect, useRef } from "react";
import { fetchGameStatusById } from "../shared/gameStatusUtils.js";

export default function useLivePropStatus(props, setProps) {
  const latestPropsRef = useRef(props);

  useEffect(() => {
    latestPropsRef.current = props;
  }, [props]);

  useEffect(() => {
    let isMounted = true;

    const checkStatuses = async () => {
      const propsToCheck = latestPropsRef.current;
      if (!propsToCheck || propsToCheck.length === 0) return;

      console.log(
        "🔄 Checking live statuses for",
        propsToCheck.length,
        "props"
      );

      const updatedProps = await Promise.all(
        propsToCheck.map(async (prop) => {
          if (["win", "loss", "push", "dnp"].includes(prop.status)) {
            return prop;
          }

          const gameStatus = await fetchGameStatusById(prop.game_id);
          console.log(`🎯 Game ID ${prop.game_id} status:`, gameStatus);

          if (gameStatus === "In Progress") {
            console.log(`✅ Setting status to "live" for ${prop.player_name}`);
            return { ...prop, status: "live" };
          }

          if (gameStatus === "Final") {
            return { ...prop, status: prop.status ?? "resolved" };
          }

          return prop;
        })
      );

      if (isMounted) {
        setProps(updatedProps);
      }
    };

    const firstTimeout = setTimeout(() => {
      checkStatuses();
    }, 3000);

    const interval = setInterval(() => {
      checkStatuses();
    }, 90000);

    return () => {
      isMounted = false;
      clearTimeout(firstTimeout);
      clearInterval(interval);
    };
  }, []);
}
