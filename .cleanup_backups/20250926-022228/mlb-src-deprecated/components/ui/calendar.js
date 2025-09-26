// src/components/ui/calendar.js
import * as React from "react";
import { format } from "date-fns";
import { DayPicker } from "react-day-picker";
import "react-day-picker/dist/style.css";

const Calendar = ({ selected, onSelect }) => {
  return (
    <div className="rounded-md border bg-blue-50">
      <DayPicker
        mode="single"
        selected={selected}
        onSelect={onSelect} // <-- this is critical!
        defaultMonth={selected}
        captionLayout="dropdown"
        fromYear={2020}
        toYear={new Date().getFullYear()}
      />
    </div>
  );
};

export default Calendar;
