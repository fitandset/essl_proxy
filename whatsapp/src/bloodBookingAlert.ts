export interface BloodLabBookingRecord {
  id?: unknown;
  package_name?: unknown;
  quoted_amount_inr?: unknown;
  beneficiary_name?: unknown;
  mobile?: unknown;
  email?: unknown;
  age?: unknown;
  gender?: unknown;
  collection_address?: unknown;
  pincode?: unknown;
  status?: unknown;
  pincode_listed?: unknown;
}

function textValue(value: unknown): string | null {
  if (value === null || value === undefined) {
    return null;
  }
  const text = String(value).trim();
  return text ? text : null;
}

function formatGender(value: unknown): string | null {
  const text = textValue(value);
  if (!text) {
    return null;
  }
  const normalized = text.toLowerCase();
  if (normalized === "male" || normalized === "m") {
    return "Male";
  }
  if (normalized === "female" || normalized === "f") {
    return "Female";
  }
  return text;
}

function formatAmount(value: unknown): string | null {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  const amount = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(amount)) {
    return null;
  }
  return `₹${amount}`;
}

function detailLine(label: string, value: string | null): string | null {
  if (!value) {
    return null;
  }
  return `${label}: ${value}`;
}

function coverageHeader(pincodeListed: unknown): { header: string; followUp: string | null } {
  if (pincodeListed === true || pincodeListed === "true") {
    return { header: "PIN AVAILABLE", followUp: null };
  }
  if (pincodeListed === false || pincodeListed === "false") {
    return {
      header: "PIN NOT AVAILABLE",
      followUp:
        "Follow up: this PIN is not on Thyrocare’s list. The user was not sent to book a slot.",
    };
  }
  return { header: "PIN COVERAGE UNKNOWN", followUp: null };
}

export function formatBloodBookingAlert(record: BloodLabBookingRecord): string {
  const coverage = coverageHeader(record.pincode_listed);
  const lines = [
    coverage.header,
    detailLine("Booking id", textValue(record.id)),
    detailLine("Package", textValue(record.package_name)),
    detailLine("Amount", formatAmount(record.quoted_amount_inr)),
    detailLine("Name", textValue(record.beneficiary_name)),
    detailLine("Mobile", textValue(record.mobile)),
    detailLine("Email", textValue(record.email)),
    detailLine("Age", textValue(record.age)),
    detailLine("Gender", formatGender(record.gender)),
    detailLine("Address", textValue(record.collection_address)),
    detailLine("PIN", textValue(record.pincode)),
    detailLine("Status", textValue(record.status)),
    coverage.followUp,
  ];

  return lines.filter((line): line is string => Boolean(line)).join("\n");
}
