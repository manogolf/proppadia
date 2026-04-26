# Auth Signup Onboarding

## Current Signup Flow
- Entry point: `/login`
- Component: `frontend/src/components/MemberLogin.jsx`
- Methods:
  - Email/password sign in
  - Email/password account creation (`supabase.auth.signUp`)
  - Email magic link
  - OAuth providers configured by `VITE_AUTH_OAUTH_PROVIDERS` (Google currently)

## Email Confirmation Behavior
- Supabase controls whether email confirmation is required before a new account can sign in.
- Setting location in Supabase dashboard:
  - `Authentication` -> `Providers` -> `Email` -> confirmation / verify email options
- App behavior:
  - If sign-up returns a session: user is signed in immediately.
  - If sign-up returns no session: app shows a confirm-email message and waits for a valid session.

## New User Visibility (Admin/Ops)
- View new users in Supabase dashboard:
  - `Authentication` -> `Users`
- This app currently has no built-in admin notification for new signups.

## Profile Row Behavior
- No app-managed `profiles` bootstrap path is currently implemented in this repository.
- Signup/login relies on Supabase Auth user records only.

## Access Control
- Member pages require sign-in via frontend route guard.
- Ops/admin pages still require existing ops allowlist checks and ops API token paths.
